/*******************************************************************************

    Mixin for shared iteration code

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.model.IterationMixin;

import ocean.transition;

/*******************************************************************************

    Indicates whether to mixin code for iterating over the complete channel or
    a sub-range of it

*******************************************************************************/

enum IterationKind
{
    Complete,
    Ranged
}

/*******************************************************************************

    Common code shared by all requests that implement protocol based on
    dlsproto.node.request.model.CompressedBatch

    Template Params:
        resources = host field which stores IKVRequestResources
        kind = kind of iteration (complete / ranged)
        predicate = optional predicate function to filter away some records.
            Defaults to predicate that allows everything.

*******************************************************************************/

public template ChannelIteration ( alias resources, IterationKind kind,
    alias predicate = alwaysTrue )
{
    import dlsnode.storage.StorageChannels;
    import dlsnode.storage.StorageEngine;
    import dlsnode.storage.iterator.StorageEngineStepIterator;

    /***************************************************************************

        Set to iterator over requested channel if that channel is present in
        the node. Set to null otherwise (should result in empty OK response)

    ***************************************************************************/

    private StorageEngineStepIterator iterator;

    static if (kind == IterationKind.Ranged)
    {
        /***********************************************************************

            The lower and upper bound of keys to include in the iteration. Keys
            outside of this range will be rejected (see getNext(), below). The
            keys must be set before prepareChannel() is called. The normal call
            order of the protected methods of this class ensures this
            (prepareRange() is called before prepareChannel()).

            Note that these keys are stored as slices to buffers which are
            assumed to remain constant while the request is being handled.

        ***********************************************************************/

        private cstring key_lower, key_upper;

        /***********************************************************************

            Communicates requested range to protocol implementation. This method
            is called during the stage of reading the request data from the
            client.

            Params:
                key_lower = lower bound key in requested range
                key_upper = upper bound key in requested range

        ***********************************************************************/

        override protected void prepareRange ( cstring key_lower, cstring key_upper )
        {
            this.key_lower = key_lower;
            this.key_upper = key_upper;
        }
    }

    /***************************************************************************

        Initialize the channel iterator

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        auto storage_channel = channel_name in resources.storage_channels;
        if (storage_channel is null)
        {
            this.iterator = null;
        }
        else
        {
            resources.iterator.setStorage(*storage_channel);
            this.iterator = cast(StorageEngineStepIterator) resources.iterator;
            assert (iterator);
            static if (kind == IterationKind.Ranged)
            {
                iterator.getRange(resources.suspended_job,
                        this.key_lower, this.key_upper);
            }
            else
            {
                iterator.getAll(resources.suspended_job);
            }
        }

        // even missing channel is ok, response must return empty record
        // list in that case
        return true;
    }

    /***************************************************************************

        Iterates records for the protocol

        Params:
            key = output value for next record's key
            value = output value for next record's value

        Returns:
            `true` if there was data, `false` if request is complete

    ***************************************************************************/

    override protected bool getNext (out cstring key, out cstring value)
    {
        // missing channel case
        if (this.iterator is null)
            return false;

        // loops either until match is found or last key processed
        while (true)
        {
            this.iterator.next(this.resources.suspended_job);

            resources.loop_ceder.handleCeding();

            if (this.iterator.lastKey)
                return false;

            key = iterator.key();
            value = iterator.value(this.resources.suspended_job);

            static if (kind == IterationKind.Ranged)
            {
                if (key < this.key_lower || key > this.key_upper)
                    continue;
            }

            if (predicate(key, value))
            {
                this.resources.node_info.record_action_counters
                    .increment("handled", value.length);
                return true;
            }
        }
    }
}

/*******************************************************************************

    Default predicate which allows all records to be sent to the client.

    Params:
        args = any arguments

    Returns:
        true

*******************************************************************************/

public bool alwaysTrue ( T... ) ( T args )
{
    return true;
}
