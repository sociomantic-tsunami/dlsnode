/*******************************************************************************

    Interface and base scope class containing getter methods to acquire
    resources needed by a DLS node request. Multiple calls to the same
    getter only result in the acquiring of a single resource of that type, so
    that the same resource is used over the life time of a request. When a
    request resource instance goes out of scope all required resources are
    automatically relinquished.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.model.RequestResources;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.common.request.model.IRequestResources;

import dlsnode.connection.SharedResources;

import dlsnode.storage.StorageChannels;
import dlsnode.storage.iterator.StorageEngineStepIterator;

import dlsnode.node.IDlsNodeInfo;

import dlsproto.node.request.model.DlsCommand;

import dlsnode.util.aio.AsyncIO;


/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (dlsnode.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding a couple of
    key/value-specific getters. It also includes DlsCommand.Resources which
    is necessary for protocol classes.

*******************************************************************************/

public interface IDlsRequestResources : IRequestResources, DlsCommand.Resources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .LoopCeder LoopCeder;
    alias .StorageChannels StorageChannels;
    alias .IDlsNodeInfo IDlsNodeInfo;
    alias .StorageEngineStepIterator StorageEngineStepIterator;


    /***************************************************************************

        Storage channels getter.

    ***************************************************************************/

    StorageChannels storage_channels ( );


    /***************************************************************************

        Node info getter.

    ***************************************************************************/

    IDlsNodeInfo node_info ( );


    /***************************************************************************

        PCRE instance getter.

    ***************************************************************************/

    PCRE pcre ( );

    /**************************************************************************

        AsyncIO instance.

    **************************************************************************/

    AsyncIO async_io ( );
}



/*******************************************************************************

    Mix in a scope class called RequestResources which implements
    IRequestResources. Note that this class does not implement the additional
    methods required by IDlsRequestResources -- this is done by the derived
    class in dlsnode.connection.DlsConnectionHandler.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);

