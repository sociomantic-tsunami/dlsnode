/*******************************************************************************

    Implementation of DLS 'GetRange' request

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.GetRangeRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.GetRange;

/*******************************************************************************

    GetRange request

*******************************************************************************/

scope class GetRangeRequest : Protocol.GetRange
{
    import dlsnode.request.model.IterationMixin;
    import dlsnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Adds this.iterator and prepareChannel override to initialize it.
        Defines default `getNext` method

    ***************************************************************************/

    mixin ChannelIteration!(resources, IterationKind.Ranged);
}

