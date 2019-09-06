/*******************************************************************************

    DLS storage engine

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.StorageEngine;



/*******************************************************************************

    Imports

*******************************************************************************/

import dlsnode.storage.iterator.model.IStorageEngineStepIterator;

import swarm.node.storage.model.IStorageEngine;

import ocean.util.log.Logger;

import dlsnode.util.aio.JobNotification;
import dlsnode.util.aio.AsyncIO;
import core.stdc.time;
import Hash = swarm.util.Hash;


/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.torage.StorageEngine");
}



/*******************************************************************************

    DLS storage engine class

*******************************************************************************/

public class StorageEngine : IStorageEngine
{
    import dlsnode.storage.checkpoint.CheckpointService;
    import dlsnode.storage.FileSystemLayout;
    import dlsnode.storage.BufferedBucketOutput;

    import ocean.core.Array : concat;
    import ocean.core.Enforce;
    import ocean.io.FilePath;
    import ocean.meta.types.Qualifiers : cstring, mstring;

    import Path = ocean.io.Path;


    /***************************************************************************

        Set of files kept open for writing. The set has a maximum size. Once the
        maximum number of files have been opened, new files will replace the
        least recently used file will be closed (see BufferedBucketOutput.openFile())
        and reopened.

    ***************************************************************************/

    private class Writers
    {
        import dlsnode.storage.util.RecentFiles;


        /***********************************************************************

            Maximum number of open files to maintain for this channel.

        ***********************************************************************/

        private static immutable max_files = 3;


        /***************************************************************************

            Checkpoint service instance.

        ***************************************************************************/

        private CheckpointService checkpointer;


        /***********************************************************************

            Count of the number of BufferedBucketOutput instances created internally.
            Only used to assert that this number does not exceed the specified
            maximum.

        ***********************************************************************/

        private uint files_created;


        /***********************************************************************

            Cache of open files. A cache is used to keep the files in recently-
            used order.

            The files are identified by a hash indicating the range of records
            which they store, see SlotBucket.toHash.

        ***********************************************************************/

        private RecentFiles recent_files;


        /***********************************************************************

            Size of files' write buffer. Set in the constructor.

        ***********************************************************************/

        private size_t write_buffer_size;


        /***********************************************************************

            Constructor.

            Params:
                write_buffer_size = size of files' write buffer

        ***********************************************************************/

        public this ( size_t write_buffer_size, CheckpointService checkpointer )
        {
            this.write_buffer_size = write_buffer_size;
            this.checkpointer = checkpointer;

            this.recent_files = new RecentFiles(this.max_files);
        }


        /***********************************************************************

            Adds a record to the storage engine. If the specified record is to
            be stored in a file which is already open, that file is used.
            Otherwise, a new file is opened or the least recently used file in
            the cache is re-opened.

            Params:
                key = key of record
                value = value of record
                record_buffer = buffer used internally for rendering entire record
                                passing it to BufferedOutput.
                event = FiberSelectEvent to block the request on if needed

        ***********************************************************************/

        public void put ( hash_t key, cstring value, ref ubyte[] record_buffer,
                JobNotification suspended_job )
        {
            // Calculate file hash.
            SlotBucket sb;
            auto file_hash = sb.fromKey(key).toHash();

            // Get file from cache, recycled (LRU) file, or a pointer to null
            // (if a new file instance must be created).
            bool recently_used;
            auto file =
                this.recent_files.getRefreshOrCreate(file_hash, recently_used);

            // A null file means that the cache is being filled up and we need
            // to construct a new instance.
            if ( *file is null )
            {
                debug
                {
                    ++this.files_created;
                    assert(this.files_created <= this.max_files);
                    .log.trace("Newing {}th writer for storage engine 0x{:x}",
                        this.files_created, cast(void*)this.outer);
                }

                *file = new BufferedBucketOutput(this.checkpointer,
                       this.outer.async_io,
                       write_buffer_size);
            }

            // If the file was 1. already in the cache but has expired and
            // should now be reused and 2. not currently open, we reset its
            // directory. This is to handle the case where a channel (an
            // instance of StorageEngine) has been deleted then recycled -- the
            // files it had open for writing must be reset to write to the new
            // channel's directory.
            // (Note that all files are closed upon calling commitAndClose().)
            if ( !recently_used || !file.isOpen() )
            {
                file.setDir(this.outer.id, this.outer.channel_dir);
            }

            file.put(key, value, record_buffer, suspended_job);
        }


        /***********************************************************************

            Commit all buffered changes to disk and close all open files.

        ***********************************************************************/

        public void commitAndClose ( )
        {
            foreach ( file_hash, wrapped_file, priority; this.recent_files )
            {
                wrapped_file.value.commitAndClose();
            }
        }

        /***********************************************************************

            commit all buffered changes to disk

        ***********************************************************************/

        public void flushData ( )
        {
            foreach ( file_hash, wrapped_file, priority; this.recent_files )
            {
                wrapped_file.value.flushData();
            }
        }
    }


    /***************************************************************************

        Writers instance.

    ***************************************************************************/

    private Writers writers;


    /***********************************************************************

        Base data directory in which all channel directories live.

    ***********************************************************************/

    private cstring global_data_dir;


    /***********************************************************************

        Channel directory.

        In the constructor the channel directory is composed from the base
        directory of the storage channels and the id of this instance.

    ***********************************************************************/

    private mstring channel_dir;


    /**************************************************************************

        AsyncIO instance

    **************************************************************************/

    private AsyncIO async_io;


    /***********************************************************************

        Constructor.

        Params:
            id = identifier string for this instance
            global_data_dir = base data directory
            write_buffer_size = size of write buffer (in bytes)
            checkpointer = CheckpointService instance
            async_io = AsyncIO instance

    ***********************************************************************/

    public this ( cstring id, cstring global_data_dir,
        size_t write_buffer_size, CheckpointService checkpointer,
        AsyncIO async_io )
    {
        assert(async_io);
        this.global_data_dir = global_data_dir;
        this.writers = new Writers(write_buffer_size, checkpointer);
        this.async_io = async_io;

        super(id);
    }


    /***********************************************************************

        Initialiser. Called from the super constructor, as well as when a
        storage engine is re-used from the pool.

    ***********************************************************************/

    override public void initialise ( cstring id )
    {
        super.initialise(id);

        this.channel_dir.concat(this.global_data_dir, "/", super.id);
        this.createChannelDir();
    }


    /***********************************************************************

        Puts a record into the database.

        Params:
            key   = record key
            value = record value
            record_buffer = buffer used internally for rendering entire record
                            passing it to BufferedOutput.
            suspended_job = suspended_job to block
                and wait on for IO to happen

        Returns:
            this instance

    ***********************************************************************/

    typeof(this) put ( cstring key, cstring value, ref ubyte[] record_buffer,
            JobNotification suspended_job )
    {
        this.writers.put(Hash.straightToHash(key), value, record_buffer,
                suspended_job);

        return this;
    }

    /***********************************************************************

        Puts a record into the database.

        Params:
            key   = record key
            value = record value
            record_buffer = buffer used internally for rendering entire record
                            passing it to BufferedOutput.
            event = event to block and wait on for IO to happen

        Returns:
            this instance

    ***********************************************************************/

    typeof(this) put ( time_t key, char[] value, ref ubyte[] record_buffer,
           JobNotification waiting_context )
    {
        this.writers.put(key, value, record_buffer,
                waiting_context);

        return this;
    }

    /***********************************************************************

        Initialises a step-by-step iterator over the keys of all records in
        the database.

        Params:
            iterator = iterator to initialise
            suspended_job = JobNotification instance to
                block the caller on.

    ***********************************************************************/

    public typeof(this) getAll ( IStorageEngineStepIterator iterator,
            JobNotification suspended_job )
    {
        (cast(IStorageEngineStepIterator)iterator).getAll(suspended_job);

        return this;
    }


    /***********************************************************************

        Initialises a step-by-step iterator over the keys of all records in
        the database in the specified range.

        Params:
            iterator = iterator to initialise
            min = minimum hash to iterate over
            max = maximum hash to iterate over
            suspended_job = JobNotification instance to
                block the caller on.

    ***********************************************************************/

    public typeof(this) getRange ( IStorageEngineStepIterator iterator,
            cstring min, cstring max, JobNotification suspended_job )
    {
        (cast(IStorageEngineStepIterator)iterator).getRange(suspended_job, min, max);

        return this;
    }


    /***********************************************************************

        Closes database. Commits any pending writes to disk.

        (Called from IStorageChannels when removing a channel or shutting
        down the node. In the former case, the channel is clear()ed then
        close()d. In the latter case, the channel is only close()d.)

        Returns:
            this instance

    ***********************************************************************/

    public override typeof(this) close ( )
    {
        .log.info("Closing DLS channel '{}'", super.id);

        this.commitAndClose();

        return this;
    }


    /***********************************************************************

        Removes all records from database.

        (Called from IStorageChannels when removing a channel.)

        Returns:
            this instance

    ***********************************************************************/

    public override typeof(this) clear ( )
    {
        .log.info("Clearing (deleting) DLS channel '{}'", super.id);

        this.commitAndClose();
        FileSystemLayout.removeFiles(this.channel_dir);
        this.removeChannelDir();

        return this;
    }


    /***************************************************************************

        Stub method to satisfy the interface. DLS node doesn't keep track of
        this.

        Returns:
            always 0

    ***************************************************************************/

    public ulong num_records ( )
    {
        return 0;
    }


    /***************************************************************************

        Stub method to satisfy the interface. DLS node doesn't keep track of
        this.

        Returns:
            always 0

    ***************************************************************************/

    public ulong num_bytes ( )
    {
        return 0;
    }


    /***********************************************************************

        commit pending data to write and closes the file

    ***********************************************************************/

    public void commitAndClose ( )
    {
        this.writers.commitAndClose();
    }

    /***********************************************************************

        commit pending data to file

    ***********************************************************************/

    public void flushData ( )
    {
        this.writers.flushData();
    }


    /***************************************************************************

        Returns:
            storage engine channel directory

    ***************************************************************************/

    public cstring working_dir ( )
    {
        return this.channel_dir;
    }


    /***********************************************************************

        Creates the channel folder for this storage channel, if it doesn't
        already exist.

        Note: this.createChannelDir creates the channel directory for a
        single channel, whereas super.createWorkingDir creates the main data
        directory which contains the folders for all storage channels.

    ***********************************************************************/

    private void createChannelDir ( )
    {
        scope path = new FilePath(this.channel_dir);

        if ( path.exists )
        {
            enforce(path.isFolder(), typeof (this).stringof ~ ": '" ~
                                      path.toString() ~ "' - not a directory");
        }
        else
        {
            path.createFolder();
        }
    }


    /***************************************************************************

        Removes the channel directory. This method will do nothing if the
        directory is not empty.

    ***************************************************************************/

    private void removeChannelDir ( )
    {
        if ( Path.exists(this.channel_dir) )
        {
            Path.remove(this.channel_dir);
        }
    }
}

