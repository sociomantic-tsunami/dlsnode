/*******************************************************************************

    Implementation of DLS 'GetChannelSize' request

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.GetChannelSizeRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.GetChannelSize;
import ocean.transition;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetChannelSizeRequest : Protocol.GetChannelSize
{
    import dlsnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();
}
