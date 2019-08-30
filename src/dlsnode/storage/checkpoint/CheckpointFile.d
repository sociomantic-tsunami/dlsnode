/*******************************************************************************

    Checkpoint file of dlsnode buckets.

    This is a text file where each line corresponds to one bucket file. The
    lines contain the following tokens (`:` separated):

    * channel_name
    * first hash of in the bucket
    * last position we know we have commited to file system

    Entire file stored on disk is being refered as a "checkpoint". After the
    failure, on the next startup node can go trough it and restore values of
    all buckets to the last known values.

    The intention is that on startup we can iterate through this file
    and truncate all files to the last commited location.

    This file originates from the dmqnode.storage.engine.overflow.IndexFile

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.checkpoint.CheckpointFile;

import ocean.util.log.Logger;
import ocean.transition;

/******************************************************************************

    Static module logger.

******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.storage.checkpoint.CheckpointFile");
}

class CheckpointFile
{
    import core.sys.posix.stdio: fdopen, fclose;
    import core.stdc.stdio: FILE, EOF, fscanf, fprintf, feof, rewind, clearerr, fflush;
    import core.stdc.stdlib: free;
    import ocean.io.FilePath;

    import ocean.sys.SignalMask;
    import core.sys.posix.signal: SIGABRT, SIGSEGV, SIGILL, SIGBUS;

    import ocean.sys.ErrnoException;
    import ocean.core.Array;
    import ocean.io.device.File;

    import dlsnode.util.PosixFile;
    import dlsnode.storage.FileSystemLayout;

    /***************************************************************************

        Signals that should not be blocked because the program should be
        terminated immediately if one of these is raised.

    ***************************************************************************/

    public static immutable signals_dontblock = [SIGABRT, SIGSEGV, SIGBUS, SIGILL];

    /***************************************************************************

        Signal set to block all signals except unblocked_signals while
        formatted file I/O functions are running, which cannot be restarted or
        recovered if interrupted by a signal.

    ***************************************************************************/

    private static SignalSet fmt_io_signal_blocker;


    /**************************************************************************

        ErrnoException instance.

    **************************************************************************/

    private ErrnoException exception;

    static this ( )
    {
        this.fmt_io_signal_blocker = this.fmt_io_signal_blocker; // Pacify compiler
        this.fmt_io_signal_blocker.setAll();
        this.fmt_io_signal_blocker.remove(this.signals_dontblock.dup);
    }


    /***************************************************************************

        Underlying POSIX file instance.

    ***************************************************************************/

    private PosixFile file;


    /***************************************************************************

        The file as stdio FILE stream.

    ***************************************************************************/

    private FILE* stream;


    /***************************************************************************

        Constructor.

        Params:
            dir  = working directory
            name = file name

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public this ( cstring dir, cstring name )
    {
        .log.info("Openning file: {}/{}", dir, name);
        this.file = new PosixFile(dir, name);
        this.exception = new ErrnoException;
    }

    /**************************************************************************

        Opens a CheckpointFile for reading and writting. The file is created
        if it doesn't exist.

    **************************************************************************/

    public void open ()
    {
        this.file.open();
        this.stream = fdopen(this.file.fd, "w+".ptr);
        this.exception.enforce(this.stream !is null, "unable to fdopen",
                "fdopen");
    }

    /***************************************************************************

        Closes a CheckpointFile.

    ***************************************************************************/

    public void close ()
    {
        fclose(this.stream);
    }

    /***************************************************************************

        Parses the checkpoint file and calls got_channel for each channel in the
        file. The channel_name and channel arguments passed to got_channel are
        validated: channel_name is a valid queue channel name, and channel is
        validated according to the criteria of its invariant.

        Params:
            got_channel = called for each channel with validated channel_name
                          and channel; nline is the line number

        Throws:
            ErrnoException on file I/O error or bad checkpoint file content (parse
            error or values that would make the ChannelMetadata invariant fail).

    ***************************************************************************/

    public void readLines ( scope void delegate ( cstring channel_name,
                                            size_t bucket_start, size_t bucket_offset,
                                            uint nline ) got_bucket )
    {
        rewind(this.stream);

        for (uint nline = 1;; nline++)
        {
            int name_start, name_end;
            size_t bucket_start, bucket_offset;
            char* channel_name = null;

            scope (exit)
            {
                /*
                 * fscanf() allocates channel_name via malloc() on a match or
                 * leaves it untouched (null) on mismatch.
                 */
                if (channel_name) free(channel_name);
            }

            int n;
            this.fmt_io_signal_blocker.callBlocked(
                /*
                 * Special fscanf format tokens:
                 *   - The leading ' ' skips leading white space.
                 *   - %n stores the current position in the input string in the
                 *     argument so that
                 *     channel_name.length = name_end - name_start.
                 *   - %m matches a string, stores it in a buffer allocated by
                 *     malloc and stores a pointer to that buffer in the
                 *     argument.
                 *   - [_0-9a-zA-Z-] makes %m match only strings that consist of
                 *     the characters '_', '0'-'9', 'a'-'z', 'A'-'Z' or '-',
                 *     which ensures the string is a valid dls channel name.
                 */
                n = fscanf(this.stream,
                           " %n%m[_0-9a-zA-Z-]%n %lu %lu".ptr,
                           &name_start, &channel_name, &name_end,
                           &bucket_start, &bucket_offset)
            );

            switch (n)
            {
                case 3:
                    got_bucket(channel_name[0 .. name_end - name_start], bucket_start, bucket_offset, nline);
                    break;

                case EOF:
                    this.exception.enforce(feof(this.stream) != 0, "Error reading checkpoint file.",
                            "fscanf");
                    return;

                default:
                    this.exception.enforce(!feof(this.stream), "Unexpected end of file",
                            "fscanf");
                    istring[] errmsg =
                    [
                        "Invalid channel name"[],
                        "Invalid bucket start",
                        "Invalid bucket offset"
                    ];
                    this.exception.msg = errmsg[n];
                    this.exception.file = this.file.name;
                    this.exception.line = nline;
                    throw this.exception;
            }
        }
    }

    /***************************************************************************

        Resets the checkpoint file to be empty, then writes lines to the checkpoint file.

        Calls iterate, which in turn should call writeln for each line that
        should be written to the checkpoint file. All signals except the ones in
        this.signals_dontblock are blocked while iterate is executing. Flushes
        the checkpoint file output after iterate has returned (not if it throws).

        Params:
            iterate = called once with a writeln delegate as argument; each call
                      of writeln writes one line to the checkpoint file

        Throws:
            ErrnoException on file I/O error.

    ***************************************************************************/

    public void writeLines (
            scope void delegate (
                void delegate ( cstring channel_name,
                                size_t bucket_start,
                                size_t bucket_offset ) writeln ) iterate )
    {
        this.reset();

        this.fmt_io_signal_blocker.callBlocked({
            iterate((cstring name, size_t bucket_start, size_t bucket_offset)
            {
                int n = fprintf(this.stream, "%.*s %lu %lu\n".ptr,
                                name.length, name.ptr,
                                bucket_start, bucket_offset);

                this.exception.enforce(n >= 0, "error writing checkpoint",
                    "fprintf");
            });

            auto ret = fflush(this.stream);
            this.exception.enforce(ret == 0, "error flushing checkpoint",
                "fflush");
        }());
    }

    /**************************************************************************

        Reads the checkpoint file and truncates the bucket at the position that's
        written inside it.
        This should be performed at node startup, to truncate any content
        that was not secured by checkpoint.

        Params:
            dir = directory where checkpoint log is stored
            file_name = file name of the checkpoint log

        Returns:
            true if the checkpoint file was found and processed,
            false otherwise

    ****************************************************************************/

    static public bool truncateBuckets (cstring dir, cstring file_name)
    {
        cstring file_path = FilePath.join(dir, file_name);

        try
        {
            scope CheckpointFile checkpoint_file = new CheckpointFile(dir, file_name);
            checkpoint_file.open();

            checkpoint_file.readLines(
                    ( cstring channel_name, size_t bucket_start, size_t bucket_offset, uint nline )
                    {
                        try
                        {
                            // Let's find the path for this one
                            SlotBucket sb;
                            sb.fromKey(bucket_start);

                            mstring slot_str, bucket_str;
                            mstring base_path;
                            base_path.concat(dir, "/", channel_name);

                            FileSystemLayout.getBucketPathFromParts(base_path, slot_str,
                                bucket_str, sb);

                            .log.info("Found bucket {}: {} {} {} {}",
                                nline, channel_name, bucket_start, bucket_offset, bucket_str);

                            scope path = new FilePath(bucket_str);

                            if (path.exists)
                            {
                                // if there's a need to truncate bucket,
                                // truncate it.
                                scope file = new File(bucket_str, File.ReadWriteExisting);

                                scope(exit)
                                {
                                    file.sync();
                                    file.close();
                                }

                                if (file.length > bucket_offset)
                                {
                                    .log.info("Truncatting bucket {}. {} -> {}",
                                            bucket_str, file.length, bucket_offset);
                                    file.truncate(bucket_offset);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            .log.error("Couldn't process checkpoint entry: {}", e.message());
                        }

                    }
            );

            checkpoint_file.close();
            return true;
        }
        catch (Exception e)
        {
            .log.error("Couldn't process checkpoint file: {}", e.message());
            return false;
        }
    }


    /***************************************************************************

        Resets the error indicator when the file is truncated to be empty.

    ***************************************************************************/

    public void reset ( )
    {
        this.file.reset();
        clearerr(this.stream);
    }
}
