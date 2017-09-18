/*******************************************************************************

    Implementation of DLS 'PutBatch' request

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.PutBatchRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.PutBatch;

import ocean.util.log.Logger;
import ocean.transition;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.request.PutBatchRequest");
}

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class PutBatchRequest : Protocol.PutBatch
{
    import dlsnode.storage.StorageEngine;
    import dlsnode.request.model.ConstructorMixin;

    import ocean.core.TypeConvert : downcast;

    /***************************************************************************

        Used to cache storage channel current request operates on

    ***************************************************************************/

    private StorageEngine storage_channel;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Caches requested channel

    ***************************************************************************/

    final override protected bool prepareChannel ( cstring channel_name )
    {
        if (!super.prepareChannel(channel_name))
            return false;
        auto storage_channel = this.resources.storage_channels.getCreate(channel_name);
        this.storage_channel = downcast!(StorageEngine)(storage_channel);
        if (this.storage_channel is null)
            return false;
        return true;
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
        this.storage_channel.put(key, value, *this.resources.record_buffer,
                this.resources.waiting_context);

        this.resources.node_info.record_action_counters
            .increment("handled", value.length);

        return true;
    }
}
