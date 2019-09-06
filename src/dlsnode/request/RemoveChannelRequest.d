/*******************************************************************************

    Implementation of DLS 'RemoveChannel' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.RemoveChannelRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.RemoveChannel;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class RemoveChannelRequest : Protocol.RemoveChannel
{
    import dlsnode.request.model.ConstructorMixin;
    import ocean.meta.types.Qualifiers : cstring;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Must remove the specified channel from the storage engine.
        Any failure is considered critical.

        Params:
            channel_name = name of channel to be removed

    ***************************************************************************/

    final override protected void removeChannel ( cstring channel_name )
    {
        this.resources.storage_channels.remove(channel_name);
    }
}
