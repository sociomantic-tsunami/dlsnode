/******************************************************************************

    Custom DlsClient with the support for ignoring handshake,
    clearing the existing node lists, support for PutBatch request and
    fiber-blocking request execution (via the ScopeRequests plugin)

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.connection.client.DlsClient;


/******************************************************************************

    Imports

******************************************************************************/

import ocean.io.compress.lzo.LzoChunkCompressor;
import ocean.core.Verify;

import SwarmClient = dlsproto.client.DlsClient;

import swarm.client.model.ClientSettings;
import swarm.util.ExtensibleClass;
import swarm.client.plugins.ScopeRequests;
import swarm.client.connection.RequestOverflow;

import dlsproto.client.legacy.DlsConst;
import dlsproto.client.legacy.internal.RequestSetup;
import dlsproto.client.legacy.internal.registry.DlsNodeRegistry;
import dlsproto.client.legacy.internal.connection.DlsNodeConnectionPool;
import dlsproto.client.legacy.internal.connection.SharedResources;

/******************************************************************************

    Custom DlsNodeConnectionPool with hack to ignore handshake failure.

******************************************************************************/

class RedistributionPool: DlsNodeConnectionPool
{

    /***************************************************************************

        Constructor

        Params:
            settings = client settings instance
            epoll = selector dispatcher instances to register the socket and I/O
                events
            address = node address
            port = node service port
            lzo = lzo chunk de/compressor
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            shared_resources = shared resources instance
            error_reporter = error reporter instance to notify on error or
                timeout

    ***************************************************************************/

    public this ( ClientSettings settings, EpollSelectDispatcher epoll,
        mstring address, ushort port, LzoChunkCompressor lzo,
        IRequestOverflow request_overflow,
        SharedResources shared_resources,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        super (settings, epoll, address, port, lzo, request_overflow,
            shared_resources, error_reporter);
    }


    /**************************************************************************

        Returns always true to ignore handshake requirement.

    **************************************************************************/

    override public bool api_version_ok ( )
    {
        return true;
    }
}

/******************************************************************************

    Custom DlsNodeRegistry with support for clearing the nodes list.

******************************************************************************/

class RedistributionRegistry: DlsNodeRegistry
{

    /***************************************************************************

        Constructor

        Params:
            epoll = selector dispatcher instance to register the socket and I/O
                events
            settings = client settings instance
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            error_reporter = error reporter instance to notify on error or
                timeout

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ClientSettings settings,
        IRequestOverflow request_overflow,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        super (epoll, settings, request_overflow,
            error_reporter);
    }


    /***************************************************************************

        Creates a new instance of the DLS node request pool class.

        Params:
            address = node address
            port = node service port

        Returns:
            new NodeConnectionPool instance

    ***************************************************************************/

    override protected NodeConnectionPool newConnectionPool ( mstring address, ushort port )
    {
        return new RedistributionPool(this.settings, this.epoll,
            address, port, this.lzo, this.request_overflow,
            this.shared_resources, this.error_reporter);
    }


    /***************************************************************************

        Removes all nodes from the registry.

    ***************************************************************************/

    public void clear ( )
    {
        this.nodes.list.length = 0;
        this.nodes.map.clear();
    }
}

/******************************************************************************

    Custom DlsClient with the support for ignoring handshake,
    clearing the existing node lists, support for PutBatch request and
    fiber-blocking request execution (via the ScopeRequests plugin)

******************************************************************************/

class DlsClient: SwarmClient.DlsClient
{
    import swarm.Const: NodeItem;

    mixin ExtensibleClass!(ScopeRequestsPlugin);


    /***************************************************************************

        Constructor.

        Params:
            epoll = EpollSelectDispatcher instance.

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll )
    {

        super (epoll);
        this.setPlugins(new ScopeRequestsPlugin);
    }

    /**************************************************************************

        Factory method for creating DlsNodeRegistry instance. Returns an
        instance of `RedistributionRegistry`

    **************************************************************************/

    protected override DlsNodeRegistry newDlsNodeRegistry ( EpollSelectDispatcher epoll,
        ClientSettings settings, IRequestOverflow request_overflow,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        return new RedistributionRegistry(epoll, settings, request_overflow,
            error_reporter);
    }

    /***************************************************************************

        Removes all nodes from the registry.

    ***************************************************************************/

    public void clearNodes ( )
    {
        auto dls_registry = cast(RedistributionRegistry)this.registry;
        dls_registry.clear();
    }


    /**************************************************************************

        PutBatch Request

    **************************************************************************/

    private struct PutBatch
    {
        mixin RequestBase;
        mixin IODelegate;
        mixin Channel;

        mixin RequestParamsSetup;
    }


    /**************************************************************************

        Creates a PutBatch Request

        Params:
            channel = channel to perform the request on
            input = input delegate
            notifier = notifier callback delegate

        Returns:
            PutBatch request instance

    **************************************************************************/

    public PutBatch putBatch ( cstring channel,
            scope RequestParams.PutBatchDg input,
            scope RequestNotification.Callback notifier )
    {
        return *PutBatch(DlsConst.Command.E.PutBatch, notifier).channel(channel).
            io(input);
    }
}
