/*******************************************************************************

    Implementation of DLS 'GetAllFilter' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.GetAllFilterRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.GetAllFilter;

/*******************************************************************************

    Request handler

*******************************************************************************/

scope class GetAllFilterRequest : Protocol.GetAllFilter
{
    import dlsnode.request.model.IterationMixin;
    import dlsnode.request.model.ConstructorMixin;

    import ocean.meta.types.Qualifiers : Const, cstring;
    import ocean.text.Search;

    /***************************************************************************

        Sub-string search instance.

    ***************************************************************************/

    private SearchFruct!(Const!(char)) match;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Predicate that accepts records that match filter defined by this.match

    ***************************************************************************/

    private bool filterPredicate ( cstring key, cstring value )
    {
        return this.match.forward(value) < value.length;
    }

    /***************************************************************************

        Adds this.iterator and prepareChannel override to initialize it
        Defines `getNext` that uses filterPredicate to filter records

    ***************************************************************************/

    mixin ChannelIteration!(resources, IterationKind.Complete, filterPredicate);

    /***************************************************************************

        Initialized regex match based on provided filter string

        Params:
            filter = filter string

    ***************************************************************************/

    final override protected void prepareFilter ( cstring filter )
    {
        this.match = search(filter);
    }
}
