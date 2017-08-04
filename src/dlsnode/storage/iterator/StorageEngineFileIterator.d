/*******************************************************************************

    Provides iteration over the files stored in the particular channel. This
    is used during the redistribution.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.iterator.StorageEngineFileIterator;

import ocean.transition;


/*******************************************************************************

    DLS storage file iterator

*******************************************************************************/

public class StorageEngineFileIterator
{
    import dlsnode.storage.FileSystemLayout : FileSystemLayout;
    import dlsnode.storage.StorageEngine;

    /***************************************************************************

        Reference to storage engine, set by setStorage() method. The instance
        is set by a method, rather than the constructor, because an instance of
        this class can be constructed by one StorageEngine, and then
        re-used by others.

    ***************************************************************************/

    private StorageEngine storage;


    /***************************************************************************

        Indicates if iteration has already started. If next() is called when
        this value is false, the iteration will be started.

    ***************************************************************************/

    private bool started;


    /***************************************************************************

        Current file path.

    ***************************************************************************/

    private mstring bucket_path;


    /***************************************************************************

        Hash of first record in the current bucket. Used by the
        FileSystemLayout.getNextBucket() method.

    ***************************************************************************/

    private hash_t current_bucket_start;


    /***************************************************************************

        Indicates if the end of the channel was reached while iterating over
        the files.

    ***************************************************************************/

    private bool end_of_channel;


    /***************************************************************************

        Gets the file path of the current bucket the iterator is pointing
        to.

        Returns:
            current value

    ***************************************************************************/

    public cstring file_path ( )
    {
        return this.bucket_path;
    }

    /***************************************************************************

        The first key which this bucket would cover (i.e. the lower range of the
        bucket)

        Returns:
            first key which this bucket would cover

    ***************************************************************************/

    public hash_t bucket_first_key ( )
    {
        return this.current_bucket_start;
    }

    /***************************************************************************

        Advances the iterator to the next file or to the first file in
        the storage engine, if this.started is false.

    ***************************************************************************/

    public void next ( )
    in
    {
        assert(this.storage, typeof(this).stringof ~ ".next: storage not set");
    }
    body
    {
        if ( !this.started )
        {
            // if iteration was not yet started, get the first bucket in the
            // channel.

            this.started = true;

            this.end_of_channel = FileSystemLayout.getFirstBucket(
                this.storage.working_dir,
                this.bucket_path, this.current_bucket_start);

            return;
        }

        hash_t next_bucket_start;
        this.end_of_channel = FileSystemLayout.getNextBucket(
            this.storage.working_dir, this.bucket_path,
            next_bucket_start,
            this.current_bucket_start, hash_t.max);

        if ( !this.end_of_channel )
        {
            this.current_bucket_start = next_bucket_start;
        }
    }


    /***************************************************************************

        Tells whether the current bucket pointed to by the iterator is the last
        in the iteration.

        Returns:
            true if the current bucket is the last in the iteration

    ***************************************************************************/

    public bool lastBucket ( )
    {
        return this.end_of_channel;
    }


    /***************************************************************************

        Performs required reset behaviour.

    ***************************************************************************/

    private void reset ( )
    in
    {
        assert(this.storage, typeof(this).stringof ~ " - storage not set");
    }
    body
    {
        this.started = false;

        this.bucket_path.length =  0;

        this.current_bucket_start = 0;
        this.end_of_channel = false;
    }


    /***************************************************************************

        Storage initialiser.

        Params:
            storage = storage engine to iterate over

    ***************************************************************************/

    public void setStorage ( StorageEngine storage )
    {
        this.storage = storage;
        this.reset();
    }
}
