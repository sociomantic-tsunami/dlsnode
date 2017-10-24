/*******************************************************************************

    Implementation of DLS 'Put' request

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.PutRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.Put;
import ocean.transition;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class PutRequest : Protocol.Put
{
    import dlsnode.request.model.ConstructorMixin;
    import dlsnode.storage.StorageEngine;

    import ocean.core.TypeConvert : downcast;
    import swarm.util.RecordBatcher;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Verifies that this node is allowed to store records of given size

        Params:
            size = size to check

        Returns:
            'true' if size is allowed

    ***************************************************************************/

    final override protected bool isSizeAllowed ( size_t size )
    {
        // Don't allow records larger than batch size
        return size <= RecordBatch.DefaultMaxBatchSize;
    }

    /***************************************************************************

        Tries storing record in DLS and reports success status

        Params:
            channel = channel to write record to
            key = record key
            value = record value

        Returns:
            'true' if storing was successful

    ***************************************************************************/

    final override protected bool putRecord ( cstring channel, cstring key, cstring value )
    {
        this.resources.node_info.record_action_counters
            .increment("handled", value.length);

        auto storage_channel = this.resources.storage_channels.getCreate(channel);
        if (storage_channel is null)
            return false;

        auto dls_channel = downcast!(StorageEngine)(storage_channel);
        assert(dls_channel);
        dls_channel.put(key, value, *this.resources.record_buffer,
                this.resources.suspended_job);

        return true;
    }
}

