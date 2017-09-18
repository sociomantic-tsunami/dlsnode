/******************************************************************************

    Output implementation for DLS storage engine

    Implements a set of static methods to read records from a log file storage
    engine.

    Database file organization

    The base directory contains numbered subdirectories, referred to as "slot
    directories" or "slots". Each slot again contains numbered files, referred
    to as "bucket files" or "buckets". Each bucket file contains a sequence of
    records where each record consists of a header, which contains the record
    key and the value data byte length, followed by the value data. The file
    structure looks like this:

    base_dir/
        slot1/
            bucket1
            bucket2
            ...
        slot2/
            bucket1
            bucket2
            ...
        ...

    The bucket which contains a record and the slot which contains that bucket
    are determined from the record key as follows.

    The key type is a 64-bit unsigned integer type which is identical to a 16
    digit hexadecimal number.

    A range of keys where only the last 3 hexadecimal digits vary is associated
    to a single bucket. That means that a bucket corresponds to a sequence of
    4096 keys.

    Of the remaining 13 hexadecimal digits, the lowest 3 denote the bucket file.
    The remaining 10 hexadecimal digits denote the slot folder.

    Examples:
    1. Key 0x12345678abcdef00 is located in bucket cde in slot 12345678ab.
    2. The keys in the range from 0x12345678abcde000 to 0x12345678abcdefff are
       located in bucket cde in slot 12345678ab.
    3. The keys in the range from 0x12345678abcd0000 to 0x12345678abcd8FFF are
       located in buckets cd0, cd1, cd2..., cd8 in slot 12345678ab.
    4. The keys in the range from 0x1234567810000000 to 0x1234567812348FFF are
       located in
        - buckets 000 to FFF in slot 1234567810 and 1234567811,
        - buckets 000 to 348 in slot 1234567812.

    As a result of the slot/bucket association method for a key width of 64 bits
    the base directory contains up to 2^40 (10 hex digits) slot directories and
    each slot directory contains up to 4096 (3 hex digits) bucket files.

    Each slot directory and bucket file is created on the first write request
    with a record key associated to them.

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

 ******************************************************************************/

module dlsnode.storage.FileSystemLayout;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Hash = swarm.util.Hash;

import dlsnode.storage.BucketFile;
import dlsnode.storage.Record;

import ocean.core.Array;
import ocean.core.Enforce;

import ocean.io.serialize.SimpleStreamSerializer;

import ocean.text.convert.Formatter;

import ocean.io.model.IConduit: IOStream, InputStream, OutputStream;

import ocean.io.device.File;

import ocean.io.FilePath;

import ocean.core.ExceptionDefinitions: IOException;

import Integer = ocean.text.convert.Integer : toUlong;

import ocean.util.log.Logger;

import ocean.stdc.posix.sys.stat;

debug ( FileSystemLayout ) import ocean.io.Stdout;

version (UnitTest)
{
    import ocean.core.Test;
}

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.storage.FileSystemLayout");
}



public class FileSystemLayout
{
    /**************************************************************************

        Private constructor to prevent instantiation. The public interface of
        this class is in its static methods.

     **************************************************************************/

    private this ( ) { }


    /**************************************************************************

        Definition of the range of hexadecimal and binary digits of a key which
        correspond to the slot and bucket and the remaining key digits.
        See description at the top.

     **************************************************************************/

    public const struct SplitBits
    {
        static const uint total_digits  = hash_t.sizeof * 2,
                          key_digits    = 3,            // 4095 keys per bucket
                          bucket_digits = 3,            // 4095 buckets per slot
                          slot_digits   = total_digits -
                                          (key_digits + bucket_digits),

                          key_bits      = key_digits * 4,
                          bucket_bits   = bucket_digits * 4,
                          slot_bits     = slot_digits * 4;

//        pragma(msg, "total hex digits = " ~ total_digits.stringof);
//        pragma(msg, "slot digits = " ~ slot_digits.stringof);
//        pragma(msg, "bucket digits = " ~ bucket_digits.stringof);
//        pragma(msg, "key digits = " ~ key_digits.stringof);
    }

    /***************************************************************************

        Interface to the file system with methods for finding slot folders and
        bucket files within specified ranges.

    ***************************************************************************/

    private interface IFileSystem
    {
        /***********************************************************************

            Scans the given base directory for the lowest matching slot folder
            within the specified range. If a suitable match is found, its value
            is returned via the ref min_slot argument.

            Params:
                base_dir = base directory containing slot folders
                found_slot = receives value of matching slot on success
                    (hash_t.min otherwise) *
                min_slot = value of minimum slot allowed *
                max_slot = value of maximum slot allowed *

            * The slot values are specified as hashes where the lowest
              SplitBits.slot_bits contain the slot value.

            Returns:
                true if no slot folder exists in the base dir within the
                specified range

        ***********************************************************************/

        bool findFirstSlotDirectory ( cstring base_dir, out hash_t found_slot,
            hash_t min_slot, hash_t max_slot );

        /***********************************************************************

            Scans the given slot directory (inside the base directory) for
            bucket files starting at the specified minimum value.

            Params:
                base_dir = base directory containing slot folders
                slot_dir = slot folder to scan
                found_bucket = receives value of matching bucket on success
                    (hash_t.min otherwise) *
                min_bucket = value of minimum bucket allowed *
                max_bucket = value of maximum bucket allowed *

            * The bucket values are specified as hashes where the lowest
              SplitBits.bucket_bits contain the bucket value.

            Returns:
                true if no bucket file exists in the slot folder within the
                specified range

        ***********************************************************************/

        bool findFirstBucketFile ( cstring base_dir, cstring slot_dir,
            out hash_t found_bucket, hash_t min_bucket, hash_t max_bucket );
    }

    /**************************************************************************

        Gets the filename of the first bucket file in the given base directory.

        Params:
            base_dir = base directory
            path = string into which the filename of the first bucket file will
                be written
            bucket_start = if a bucket file is found, receives the value of the
                first (theoretical) hash in that bucket

        Returns:
            true if no bucket files exist in the base dir or sub-directories
            (i.e. the storage channel is empty)

     **************************************************************************/

    static public bool getFirstBucket ( cstring base_dir, ref mstring path,
        out hash_t bucket_start )
    {
        return getFirstBucketInRange(base_dir, path, bucket_start, hash_t.min,
            hash_t.max);
    }

    /**************************************************************************

        Gets the filename of the first bucket file in the given base directory
        and within the specified hash range.

        Params:
            base_dir = base directory
            path = string into which the filename of the first bucket file will
                be written
            bucket_start = if a bucket file is found, receives the value of the
                first (theoretical) hash in that bucket
            min_hash = start of search range
            max_hash = end of search range

        Returns:
            true if no bucket files exist in the base dir or sub-directories
            within the specified hash range

     **************************************************************************/

    static public bool getFirstBucketInRange ( cstring base_dir, ref mstring path,
        out hash_t bucket_start, hash_t min_hash, hash_t max_hash )
    {
        // Remove key number from min & max hashes (so just the slot/bucket
        // numbers remain)
        hash_t min_bucket_slot = min_hash >> SplitBits.key_bits;
        hash_t max_bucket_slot = max_hash >> SplitBits.key_bits;

        hash_t next_bucket_slot;
        scope fs = new FileSystem;
        auto empty = getFirstBucket_(fs, base_dir, path, next_bucket_slot,
            min_bucket_slot, max_bucket_slot);
        if ( !empty )
        {
            if ( next_bucket_slot >= min_bucket_slot && next_bucket_slot <= max_bucket_slot )
            {
                bucket_start = next_bucket_slot << SplitBits.key_bits;
            }
            else
            {
                empty = true;
            }
        }

        return empty;
    }


    /**************************************************************************

        Gets the full path and file name from bucket parts - slot and bucket.

        Params:
            base_dir = base directory where the bucket should live
            slot_path = output buffer for rendering slot path
            bucket_path = output buffer for rendering bucket path
            sb = SlotBucket structure containing slot and bucket

    **************************************************************************/

    static public void getBucketPathFromParts ( cstring base_dir,
            ref mstring slot_path, ref mstring bucket_path, SlotBucket sb)
    {
        slot_path.length = base_dir.length + 1 + FileSystemLayout.SplitBits.slot_digits;
        bucket_path.length = slot_path.length + 1 + FileSystemLayout.SplitBits.bucket_digits;

        // Fill slot path
        slot_path[0 .. base_dir.length] = base_dir;
        slot_path[base_dir.length] = '/';
        Hash.intToHex(sb.slot, slot_path[base_dir.length + 1 .. $]);

        // Fill bucket path
        bucket_path[0 .. slot_path.length] = slot_path;
        bucket_path[slot_path.length] = '/';
        Hash.intToHex(sb.bucket, bucket_path[slot_path.length + 1 .. $]);
    }

    unittest
    {
        cstring base_dir = "channel";
        mstring slot_path;
        mstring bucket_path;

        SlotBucket sb;

        sb.fromKey(0x0000000000000000UL);
        getBucketPathFromParts(base_dir,
                slot_path, bucket_path,
                sb);

        test!("==")(slot_path, "channel/0000000000");
        test!("==")(bucket_path, "channel/0000000000/000");

        sb.fromKey(0x570e13ebUL);
        getBucketPathFromParts(base_dir,
                slot_path, bucket_path,
                sb);

        test!("==")(slot_path, "channel/0000000057");
        test!("==")(bucket_path, "channel/0000000057/0e1");
    }

    /**************************************************************************

        Gets the filename of the next bucket file in the given base directory
        after the specified last hash, and up to the specified maximum hash. The
        first hash of the new bucket is also returned via the ref hash
        parameter.

        Params:
            base_dir = base directory
            path = string into which the filename of the first bucket file will
                be written
            bucket_start = if a bucket file is found, receives the value of the
                first (theoretical) hash in that bucket (hash_t.min otherwise)
            last_hash = a hash in the previous bucket, used to calculate the
                hash range of the next bucket
            max_hash = highest hash bucket to return

        Returns:
            true if no bucket files exist in the base dir or sub-directories
            within the specified hash range

     **************************************************************************/

    static public bool getNextBucket ( cstring base_dir,
        ref mstring path, out hash_t bucket_start,
        hash_t last_hash, hash_t max_hash = hash_t.max )
    {
        hash_t min_bucket_slot = last_hash >> SplitBits.key_bits;
        hash_t max_bucket_slot = max_hash >> SplitBits.key_bits;

        // If this is already the last bucket, then there can be no more
        if ( min_bucket_slot >= max_bucket_slot )
        {
            return true;
        }

        // Otherwise look for the next existing bucket file.
        min_bucket_slot++;
        hash_t next_bucket_slot;
        scope fs = new FileSystem;
        auto no_bucket = getFirstBucket_(fs, base_dir, path, next_bucket_slot,
            min_bucket_slot, max_bucket_slot);

        if ( !no_bucket )
        {
            // set to first hash in bucket
            bucket_start = next_bucket_slot << SplitBits.key_bits;
        }

        return no_bucket;
    }

    /**************************************************************************

        Removes all bucket files and slot directories found within the specified
        base directory.

        Params:
            base_dir = base directory to scan for slot dirs

     **************************************************************************/

    static public void removeFiles ( cstring base_dir )
    {
        scope dir_path  = new FilePath(base_dir);
        scope slot_path = new FilePath;
        scope file_path = new FilePath;

        foreach ( dir_info; dir_path )
        {
            slot_path.file = dir_info.name;
            slot_path.folder = dir_info.path;

            if ( slot_path.isFolder )
            {
                char[SplitBits.bucket_digits] first_bucket = 'Z';
                foreach ( file_info; slot_path )
                {
                    file_path.file = file_info.name;
                    file_path.folder = file_info.path;

                    file_path.remove();
                }
            }

            slot_path.remove();
        }
    }

    /***************************************************************************

        Gets the filename of the first bucket file in the given base directory
        and within the given bucket number. First the slot folders in the base
        directory are scanned for the lowest matching slot (within the specified
        range). If a suitable slot folder is found, its bucket files are scanned
        for the lowest matching bucket (again within the specified range). This
        process is reapeated until either a matching slot/bucket is found, or
        all slots/buckets have been scanned and none were within the specified
        range.

        Params:
            filesystem = interface to the filesystem, for checking the existence
                of slot folders / bucket files
            base_dir = base directory containing slot folders
            path = string into which the filename of the first bucket file found
                will be written (unchanged if none found)
            found_bucket_slot = receives value of matching slot/bucket on
                success (hash_t.min otherwise) *
            min_bucket_slot = value of minimum slot/bucket allowed *
            max_bucket_slot = value of maximum slot/bucket allowed *

        * The slot/bucket values are specified as hashes where the lowest
          SplitBits.bucket_bits contain the bucket value, and the following
          SplitBits.slot_bits contain the slot.

        Returns:
            true if no bucket files exist in the base dir's sub-directories
            within the specified range

    ***************************************************************************/

    static private bool getFirstBucket_ ( IFileSystem filesystem, cstring base_dir,
        ref mstring path, out hash_t found_bucket_slot,
        hash_t min_bucket_slot, hash_t max_bucket_slot )
    {
        auto min_slot = min_bucket_slot >> SplitBits.bucket_bits;
        auto max_slot = max_bucket_slot >> SplitBits.bucket_bits;
        hash_t slot = min_slot;

        debug ( FileSystemLayout ) Stderr.formatln("getFirstBucket_: {:x13}..{:x13} (slot {:x10}..{:x10})",
            min_bucket_slot, max_bucket_slot, min_slot, max_slot.val);

        bool no_bucket;
        do
        {
            debug ( FileSystemLayout ) Stderr.formatln("slot {:x10}", slot);

            // Find first slot directory within range
            hash_t found_slot;
            auto no_slot = filesystem.findFirstSlotDirectory(base_dir, found_slot,
                slot, max_slot);
            if ( no_slot )
            {
                debug ( FileSystemLayout ) Stderr.formatln("  no slot dir found");
                return true;
            }
            slot = found_slot;
            debug ( FileSystemLayout ) Stderr.formatln("  found slot {:x10}", slot);

            // Work out which buckets within the slot directory count as a
            // match. Generally, all buckets in a slot are valid...
            const hash_t bucket_mask = (1 << SplitBits.bucket_bits) - 1;
            hash_t min_bucket = hash_t.min;  // 000
            hash_t max_bucket = bucket_mask; // fff

            // ...but if this is the first slot in the range, respect the min
            // bucket specified...
            if ( slot == min_slot )
            {
                min_bucket = min_bucket_slot & bucket_mask;
            }
            // ...and if this is the last slot in the range, respect the max
            // bucket specified.
            if ( slot == max_slot )
            {
                max_bucket = max_bucket_slot & bucket_mask;
            }

            // Find first bucket file in slot directory within range
            char[SplitBits.slot_digits] slot_name_buf;
            auto slot_name = Hash.intToHex(slot, slot_name_buf);

            debug ( FileSystemLayout ) Stderr.formatln("  buckets {:x3}..{:x3}", min_bucket, max_bucket);

            hash_t found_bucket;
            no_bucket = filesystem.findFirstBucketFile(base_dir, slot_name, found_bucket,
                min_bucket, max_bucket);
            if ( !no_bucket )
            {
                debug ( FileSystemLayout ) Stderr.formatln("  found bucket {:x3}", found_bucket);

                // Set output parameters
                char[SplitBits.bucket_digits] bucket_name_buf;
                auto bucket_name = Hash.intToHex(found_bucket, bucket_name_buf);

                path.length = 0;
                sformat(path, "{}/{}/{}", base_dir, slot_name,
                    bucket_name);

                found_bucket_slot = (slot << SplitBits.bucket_bits) +
                    found_bucket;

                return false;
            }

            // Try again starting at the next slot
            slot++;
        }
        while ( slot <= max_slot );

        debug ( FileSystemLayout ) Stderr.formatln("  no bucket found");
        return true;
    }

    /***************************************************************************

        Filesystem scope class with methods for finding slot folders and bucket
        files within specified ranges.

    ***************************************************************************/

    private static scope class FileSystem : IFileSystem
    {
        /***********************************************************************

            Scans the given base directory for the lowest matching slot folder
            within the specified range. If a suitable match is found, its value
            is returned via the ref min_slot argument.

            Params:
                base_dir = base directory containing slot folders
                found_slot = receives value of matching slot on success
                    (hash_t.min otherwise) *
                min_slot = value of minimum slot allowed *
                max_slot = value of maximum slot allowed *

            * The slot values are specified as hashes where the lowest
              SplitBits.slot_bits contain the slot value.

            Returns:
                true if no slot folder exists in the base dir within the
                specified range

        ***********************************************************************/

        public bool findFirstSlotDirectory ( cstring base_dir, out hash_t found_slot,
            hash_t min_slot, hash_t max_slot )
        {
            scope scan_path = new FilePath(base_dir);

            hash_t first_slot;
            auto no_slot = this.findLowestSubPath(scan_path, true, first_slot, min_slot);
            if ( no_slot )
            {
                return true;
            }

            found_slot = first_slot;
            return false;
        }

        /***********************************************************************

            Scans the given slot directory (inside the base directory) for
            bucket files starting at the specified minimum value. The existence
            of every possible bucket file is checked until either one is
            discovered or all have been checked and none has been found.

            Note: this scanning algorithm is used, instead of an algorithm which
            iterates over all extant files in the slot folder, in order to avoid
            having to scan all (unordered) files every time the method is
            called. As it is, usually bucket files with all values will exist
            (in most slot folders), so it is much more efficient to simply check
            for the existence of the next file.

            Params:
                base_dir = base directory containing slot folders
                slot_dir = slot folder to scan
                found_bucket = receives value of matching bucket on success
                    (hash_t.min otherwise) *
                min_bucket = value of minimum bucket allowed *
                max_bucket = value of maximum bucket allowed *

            * The bucket values are specified as hashes where the lowest
              SplitBits.bucket_bits contain the bucket value.

            Returns:
                true if no bucket file exists in the slot folder within the
                specified range

        ***********************************************************************/

        public bool findFirstBucketFile ( cstring base_dir, cstring slot_dir,
            out hash_t found_bucket, hash_t min_bucket, hash_t max_bucket )
        {
            scope scan_path = new FilePath;
            scan_path.folder = slot_dir;
            scan_path.prepend(base_dir);

            while ( min_bucket <= max_bucket )
            {
                char[SplitBits.bucket_digits] bucket_name_buf;

                auto filename = Hash.intToHex(min_bucket, bucket_name_buf);
                scan_path.file = assumeUnique(filename);

                // Check whether bucket file exists and if it's not empty
                // and that it contains enough data for a header to be read
                stat_t file_stats;
                auto ret_val = stat(scan_path.cString().ptr, &file_stats);

                if ( ret_val == 0 && file_stats.st_size >= BucketFile.BucketHeaderSize)
                {
                    found_bucket = min_bucket;
                    return false;
                }

                // Otherwise try the next bucket file.
                min_bucket++;
            }

            return true;
        }

        /***********************************************************************

            Iterates over all children (either child folders or child files) of
            the given path in order to find the child with the lowest
            hexadecimal filename value (i.e. the child's filename simply
            converted to an integer - filenames containing non-hex characters
            are ignored). Optionally a minimum integer value can be specified,
            in order to filter out children whose filename is < the specified
            minimum.

            Params:
                path = file path to search within
                folders = flag indicating whether child folders (true) or child
                    files (false) should be scanned
                lowest = receives the value of the lowest matching file / folder
                    (or hash_t.max if no match is found)
                min = optional minimum value to scan for

            Returns:
                true if no matching folder/file exists

        ***********************************************************************/

        private bool findLowestSubPath ( FilePath path, bool folders,
            out hash_t lowest, hash_t min = hash_t.min )
        {
            bool found;
            lowest = hash_t.max;

            // Find file in directory with 'lowest' name
            foreach ( child; path )
            {
                // Check that child is of the expected type.
                if ( child.folder != folders )
                {
                    continue;
                }

                // Get integer value of child's name (handling errors)
                hash_t value;
                if (!Integer.toUlong(child.name, value, 16))
                {
                    log.warn("{}: {} found with invalid name: {}",
                        path.name, folders ? "Folder" : "File", child.name);
                    continue;
                }

                // Ignore child if value is less than the specified minimum.
                if ( value < min )
                {
                    continue;
                }

                // Update lowest value counter, if child's value is lower.
                if ( !found || value < lowest )
                {
                    found =  true;
                    lowest = value;
                }
            }

            return !found;
        }
    }
}


/*******************************************************************************

    SlotBucket struct

    Calculates and keeps slot and bucket from a key

*******************************************************************************/

public struct SlotBucket
{
    /***************************************************************************

        Value of the "slot" (channel directory sub-folder).

    ***************************************************************************/

    public hash_t slot;

    /***************************************************************************

        Value of the "bucket" (data file). See FileSystemLayout.

    ***************************************************************************/

    public hash_t bucket;

    /***************************************************************************

        Sets the slot and bucket value from the given key.

        Params:
            key = key to split into slot / bucket

        Returns:
            this instance

    ***************************************************************************/

    public typeof (this) fromKey ( hash_t key )
    {
        this.bucket = key >> FileSystemLayout.SplitBits.key_bits;
        this.slot = bucket >> FileSystemLayout.SplitBits.bucket_bits;

        return this;
    }

    /***************************************************************************

        Returns:
            hash identifier for this instance, for use as a key in hash maps and
            the like. As the bucket value is calculated by truncating the full
            key (i.e. not by masking it), this value is a unique identifier for
            the instance and can be returned directly.

            Note that as this value is derived from a time value, not a real
            pseudo-random number, it is not really a very good hash for use in a
            hashmap. However, we know that the return value is only used by the
            recent files cache (StorageEngine.Writers) which has a very small
            number of maximum elements, meaning that the efficiency of the hash
            determination is not important.

    ***************************************************************************/

    public hash_t toHash ( )
    {
        return this.bucket;
    }

    /**************************************************************************

        Returns first key that the bucket can contain.

        Returns:
            first key that the bucket can contain.

    **************************************************************************/

    public hash_t firstKey ()
    {
        return (this.slot <<
                    (FileSystemLayout.SplitBits.bucket_bits
                     + FileSystemLayout.SplitBits.key_bits))
            | (this.bucket << FileSystemLayout.SplitBits.key_bits);
    }
}


version ( UnitTest )
{
    /***************************************************************************

        Dummy file system class. Implements the interface required by the
        DLS storage engine, but instead of checking for the presence of
        files and directories in a real file system, they are mimicked by an
        internal map of slot folder numbers -> list of bucket file numbers.

    ***************************************************************************/

    scope class DummyFileSystem : FileSystemLayout.IFileSystem
    {
        /***********************************************************************

            Aliases for a set of buckets in a slot and a map of slots, indexed
            by numerical identifier.

        ***********************************************************************/

        alias ushort[] SlotBuckets;

        alias SlotBuckets[hash_t] Slots;

        /***********************************************************************

            Id -> slot map.

        ***********************************************************************/

        private Slots slots;

        /***********************************************************************

            Adds a slot to the internal map.

            Params:
                slot_num = numerical identifier of slot (asserted to be within
                    the range of possible slot numbers defined by
                    FileSystemLayout.SplitBits)
                buckets = list of buckets within the slot (asserted to be within
                    the range of possible bucket numbers defined by
                    FileSystemLayout.SplitBits)

        ***********************************************************************/

        public void addSlot ( hash_t slot_num, SlotBuckets buckets )
        {
            const max_slot = (1 << FileSystemLayout.SplitBits.slot_digits) -1;
            const max_bucket = (1 << FileSystemLayout.SplitBits.bucket_digits) -1;

            assert(slot_num <= max_slot);

            foreach ( bucket; buckets )
            {
                assert(bucket <= max_bucket);
            }

            this.slots[slot_num] = buckets;
        }

        /***********************************************************************

            Scans the given base directory for the lowest matching slot folder
            within the specified range. If a suitable match is found, its value
            is returned via the ref min_slot argument.

            Params:
                base_dir = base directory containing slot folders
                found_slot = receives value of matching slot on success
                    (hash_t.min otherwise) *
                min_slot = value of minimum slot allowed *
                max_slot = value of maximum slot allowed *

            * The slot values are specified as hashes where the lowest
              SplitBits.slot_bits contain the slot value.

            Returns:
                true if no slot folder exists in the base dir within the
                specified range

        ***********************************************************************/

        public bool findFirstSlotDirectory ( cstring base_dir, out hash_t found_slot,
            hash_t min_slot, hash_t max_slot )
        {
            auto slot_numbers = sort(this.slots.keys.dup);
            foreach ( slot; slot_numbers )
            {
                if ( slot >= min_slot && slot <= max_slot )
                {
                    found_slot = slot;
                    return false;
                }
            }

            return true;
        }

        /***********************************************************************

            Scans the given slot directory (inside the base directory) for
            bucket files starting at the specified minimum value.

            Params:
                base_dir = base directory containing slot folders
                slot_dir = slot folder to scan
                found_bucket = receives value of matching bucket on success
                    (hash_t.min otherwise) *
                min_bucket = value of minimum bucket allowed *
                max_bucket = value of maximum bucket allowed *

            * The bucket values are specified as hashes where the lowest
              SplitBits.bucket_bits contain the bucket value.

            Returns:
                true if no bucket file exists in the slot folder within the
                specified range

        ***********************************************************************/

        public bool findFirstBucketFile ( cstring base_dir, cstring slot_dir,
            out hash_t found_bucket, hash_t min_bucket, hash_t max_bucket )
        {
            hash_t slot_num;

            if (!Integer.toUlong(slot_dir, slot_num))
            {
                .log.error("Invalid slot directory name: {}", slot_dir);
                return true;
            }

            auto slot = slot_num in this.slots;
            if ( !slot ) return true;

            foreach ( bucket; *slot )
            {
                if ( bucket >= min_bucket && bucket <= max_bucket )
                {
                    found_bucket = bucket;
                    return false;
                }
            }

            return true;
        }
    }
}


unittest
{
    /***************************************************************************

        Calls FileSystemLayout.getFirstBucket_() with the specified parameters and
        asserts that the results are as expected.

        Params:
            desc = identifier string for test, used for assert messages
            fs = dummy file system instance to run tests on
            start = start hash of search range
            end = end hash of search range
            expected_found = is the test expected to find a bucket?
            expected_bucket_slot = the value of the slot/bucket which is
                expected to be found (only checked if expected_found is true)

    ***************************************************************************/

    void test ( cstring desc, DummyFileSystem fs, hash_t start, hash_t end,
        bool expected_found, hash_t expected_bucket_slot = 0 )
    in
    {
        assert(fs !is null);
        assert(start <= end);
    }
    body
    {
        static istring base_dir = "base";
        mstring path;
        hash_t found_bucket_slot;

        auto empty = FileSystemLayout.getFirstBucket_(fs, base_dir, path, found_bucket_slot,
            start, end);
        debug ( FileSystemLayout ) Stderr.formatln("found={}, path={}, bucket/slot={:x13}",
            !empty, path, found_bucket_slot);
        assert(empty != expected_found, desc ~ ": found mismatch");

        if ( !empty ) with ( FileSystemLayout.SplitBits )
        {
            assert(path[0..base_dir.length] == base_dir, desc ~ ": base path wrong");
            assert(path[base_dir.length] == '/', desc ~ ": first slash missing");

            auto slot_start = base_dir.length + 1;
            hash_t path_slot;
            Integer.toUlong(path[slot_start..slot_start + slot_digits], path_slot);
            assert(path_slot == found_bucket_slot >> bucket_bits, desc ~ ": path slot wrong");

            assert(path[slot_start + slot_digits] == '/', desc ~ ": second slash missing");

            const hash_t bucket_mask = (1 << bucket_bits) - 1;
            auto bucket_start = slot_start + slot_digits + 1;

            hash_t path_bucket;
            Integer.toUlong(path[bucket_start..bucket_start + bucket_digits],
                    path_bucket);

            assert(path_bucket == (found_bucket_slot & bucket_mask), desc ~ ": path bucket wrong");

            assert(found_bucket_slot == expected_bucket_slot, desc ~ ": found bucket/slot mismatch");
        }
    }


    // -------------------------------------------------------------------------
    // Single slot tests

    // Single bucket, exact range
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);

        test("test 1", fs, 0x0000000001001, 0x0000000001001, true, 0x0000000001001);
    }

    // Single bucket, range start
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);

        test("test 2", fs, 0x0000000001001, 0x0000000001002, true, 0x0000000001001);
    }

    // Single bucket, range end
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);

        test("test 3", fs, 0x0000000001000, 0x0000000001001, true, 0x0000000001001);
    }

    // Single bucket, before range
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);

        test("test 4", fs, 0x0000000001002, 0x0000000001003, false);
    }

    // Single bucket, after range
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x002]);

        test("test 5", fs, 0x0000000001000, 0x0000000001001, false);
    }

    // Multiple buckets, exact range
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001, 0x002, 0x003]);

        test("test 6", fs, 0x0000000001001, 0x0000000001003, true, 0x0000000001001);
    }

    // Multiple buckets, range start
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001, 0x002, 0x003]);

        test("test 7", fs, 0x0000000001001, 0x0000000001006, true, 0x0000000001001);
    }

    // Multiple buckets, range end
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001, 0x002, 0x003]);

        test("test 8", fs, 0x0000000001003, 0x0000000001006, true, 0x0000000001003);
    }

    // Multiple buckets, before range
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001, 0x002, 0x003]);

        test("test 9", fs, 0x0000000001004, 0x0000000001004, false);
    }

    // Multiple buckets, after range
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001, 0x002, 0x003]);

        test("test 10", fs, 0x0000000001000, 0x0000000001000, false);
    }


    // -------------------------------------------------------------------------
    // Multiple slot tests

    // Range spans both slots, found in first slot
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);
        fs.addSlot(0x0000000002, [0x001]);

        test("test 11", fs, 0x0000000001000, 0x0000000002fff, true, 0x0000000001001);
    }

    // Range starts after buckets in first slot, found in second slot
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);
        fs.addSlot(0x0000000002, [0x001]);

        test("test 12", fs, 0x0000000001002, 0x0000000002fff, true, 0x0000000002001);
    }

    // Range ends before buckets of first slot
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);
        fs.addSlot(0x0000000002, [0x001]);

        test("test 13", fs, 0x0000000001000, 0x0000000001000, false);
    }

    // Range ends after buckets of second slot
    {
        scope fs = new DummyFileSystem();
        fs.addSlot(0x0000000001, [0x001]);
        fs.addSlot(0x0000000002, [0x001]);

        test("test 14", fs, 0x0000000002002, 0x0000000002fff, false);
    }
}

