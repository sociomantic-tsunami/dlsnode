/*******************************************************************************

    DLS storage engine

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.StorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

import dlsnode.storage.StorageEngine;

import swarm.node.storage.model.IStorageChannels;

import ocean.util.log.Logger;

import ocean.io.select.client.FiberSelectEvent;

import ocean.transition;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.storage.StorageChannels");
}



/*******************************************************************************

    DLS storage channels class

*******************************************************************************/

public class StorageChannels : IStorageChannelsTemplate!(StorageEngine)
{
    import Hash = swarm.util.Hash;

    import dlsnode.storage.checkpoint.CheckpointService;
    import dlsnode.storage.iterator.StorageEngineStepIterator;
    import dlsnode.storage.iterator.StorageEngineFileIterator;

    import dlsnode.storage.FileSystemLayout : FileSystemLayout;
    import dlsnode.storage.BufferedBucketOutput;

    import dlsnode.util.aio.AsyncIO;
    import ocean.io.select.client.FiberSelectEvent;

    import ocean.io.FilePath;

    import ocean.sys.Environment;

    debug import ocean.io.Stdout : Stderr;


    /***************************************************************************

        Default write buffer size

    ***************************************************************************/

    public const DefaultWriteBufferSize = BufferedBucketOutput.DefaultBufferSize;


    /***************************************************************************

        Storage data directory (copied in constructor)

    ***************************************************************************/

    private FilePath dir;


    /***************************************************************************

        Logfile write buffer size in bytes

    ***************************************************************************/

    private size_t write_buffer_size;

    /***************************************************************************

        Checkpoint service instance.

    ***************************************************************************/

    private CheckpointService checkpointer;


    /**************************************************************************

        AsyncIO instance

    **************************************************************************/

    private AsyncIO async_io;


    /***************************************************************************

        Size of the file buffer to use

    ***************************************************************************/

    private size_t file_buffer_size;


    /***************************************************************************

        Constructor. If the specified data directory exists, it is scanned for
        dumped queue channels, which are loaded. Otherwise the data directory is
        created.

        Params:
            dir = data directory for DLS
            async_io = AsyncIO instance
            file_buffer_size = size of the buffer used for buffered input.
                0 indicates no buffering.
            write_buffer_size = size in bytes of file write buffer

    ***************************************************************************/

    public this ( cstring dir, CheckpointService checkpointer,
        AsyncIO async_io,
        size_t file_buffer_size,
        size_t write_buffer_size = DefaultWriteBufferSize )
    {
        // Don't set size limit on the storage channel
        const no_size_limit = 0;
        super(no_size_limit);

        this.checkpointer = checkpointer;
        this.async_io = async_io;
        this.file_buffer_size = file_buffer_size;

        this.dir = this.getWorkingPath(dir);

        if ( !this.dir.exists )
        {
            this.createWorkingDir();
        }

        this.write_buffer_size = write_buffer_size;

        this.loadChannels();
    }


    /***************************************************************************

        Creates a new instance of an iterator for this storage engine.

        Returns:
            new iterator

    ***************************************************************************/

    public StorageEngineStepIterator newIterator ( )
    {
        return new StorageEngineStepIterator(this.async_io, this.file_buffer_size);
    }


    /***************************************************************************

        Creates a new instance of an file iterator for this storage engine.

        Returns:
            new iterator

    ***************************************************************************/

    public StorageEngineFileIterator newFileIterator ( )
    {
        return new StorageEngineFileIterator;
    }


    /***************************************************************************

        Returns:
             string identifying the type of the storage engine

    ***************************************************************************/

    public override cstring type ( )
    {
        return StorageEngine.stringof;
    }


    /***************************************************************************

        Creates a new KVStorageEngine instance of the StorageEngine
        class with the specified id.

        Params:
            id = channel id

        Returns:
            new KVStorageEngine instance

    ***************************************************************************/

    protected override StorageEngine create_ ( cstring id )
    {
        return new StorageEngine(id, this.dir.toString,
            this.write_buffer_size, this.checkpointer,
            this.async_io);
    }


    /***************************************************************************

        Searches this.dir for subdirectories and retrieves the names of the
        subdirectories as storage engine identifiers.

    ***************************************************************************/

    private void loadChannels ( )
    {
        .log.info("Scanning {} for DLS directories", this.dir.toString);
        debug Stderr.formatln("Scanning {} for DLS directories",
            this.dir.toString);

        foreach ( info; this.dir )
        {
            if ( info.folder )
            {
                auto id = info.name.dup;

                .log.info("Opening DLS directory '{}'", id);
                debug Stderr.formatln("    Opening DLS directory '{}'", id);

                this.create(id);
            }
            else
            {
                .log.warn("Ignoring file '{}' in data directory {}",
                    info.name, this.dir.toString);
            }
        }

        .log.info("Finished scanning {} for DLS directories",
            this.dir.toString);
        debug Stderr.formatln("Finished scanning {} for DLS directories",
            this.dir.toString);
    }


    /***************************************************************************

        Creates a FilePath instance set to the absolute path of dir, if dir is
        not null, or to the current working directory of the environment
        otherwise.

        Params:
            dir = directory string; null indicates that the current working
                  directory of the environment should be used

        Returns:
            FilePath instance holding path

    ***************************************************************************/

    private FilePath getWorkingPath ( cstring dir )
    {
        FilePath path = new FilePath;

        if ( dir )
        {
            path.set(dir);

            if ( !path.isAbsolute() )
            {
                path.prepend(Environment.cwd());
            }
        }
        else
        {
            path.set(Environment.cwd());
        }

        return path;
    }


    /***************************************************************************

        Creates data directory.

    ***************************************************************************/

    private void createWorkingDir ( )
    {
        try
        {
            this.dir.createFolder();
        }
        catch (Exception e)
        {
            e.msg = typeof(this).stringof ~ ": Failed creating directory: " ~ e.msg;

            throw e;
        }
    }
}

