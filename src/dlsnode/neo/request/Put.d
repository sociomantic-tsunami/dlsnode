/******************************************************************************

    Put request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.neo.request.Put;

import dlsproto.node.neo.request.Put;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;
import dlsproto.common.RequestCodes;
import dlsnode.connection.neo.SharedResources;

import ocean.transition;
import ocean.core.TypeConvert: downcast;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Put command as specified by
                      the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

public void handle ( Object shared_resources, RequestOnConn connection,
        Command.Version cmdver, Const!(void)[] msg_payload )
{
    auto dls_shared_resources = downcast!(SharedResources)(shared_resources);
    assert (dls_shared_resources);

    switch (cmdver)
    {
        case 0:
            scope rq_resources = dls_shared_resources.new RequestResources;
            scope rq = new PutImpl_v0(dls_shared_resources.storage_channels, rq_resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                    ( ed.Payload payload )
                    {
                        payload.addConstant(GlobalStatusCode.RequestVersionNotSupported);
                    }
            );
            break;
    }
}

/*******************************************************************************

    Node implementation of the PutProtocol_v0.

*******************************************************************************/

private scope class PutImpl_v0: PutProtocol_v0
{
    import swarm.util.Hash;
    import dlsnode.storage.StorageChannels;
    import dlsnode.storage.StorageEngine;
    import core.stdc.time;

    /***************************************************************************

        Fiber resume code when resumed from IO system.

    ***************************************************************************/

    const NodeResumeCode = 1;

    /**************************************************************************

        Resources

    ***************************************************************************/

    SharedResources.RequestResources resources;

    /***************************************************************************

        StorageChannels instance.

    ***************************************************************************/

    private StorageChannels storage_channels;

    /***************************************************************************

        StorageChannel instance

    ***************************************************************************/

    private StorageEngine dls_channel;

    /***************************************************************************

        Constructor.

        Params:
            storage_channels = storage channels resource
            resources = resources to use during the request

    ***************************************************************************/

    public this ( StorageChannels storage_channels,
            SharedResources.RequestResources resources )
    {
        this.storage_channels = storage_channels;
        this.resources = resources;
    }

    /***************************************************************************

        Create/get the channel to put the record to.

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        auto storage_channel = this.storage_channels.getCreate(channel_name);
        if (storage_channel is null)
            return false;

        this.dls_channel = downcast!(StorageEngine)(storage_channel);
        assert(this.dls_channel);

        return true;
    }

    /***************************************************************************

        Tries storing record in DLS and reports success status

        Params:
            channel = channel to write record to
            timestamp = record's timestamp
            value = record value

        Returns:
            'true' if storing was successful

    ***************************************************************************/

    override protected bool putInStorage ( cstring channel, time_t timestamp, in void[] value )
    {
        if (!this.storage_channels.sizeLimitOk(value.length))
        {
            return false;
        }

        auto buffer = cast(ubyte[])*this.resources.getVoidBuffer();

        auto job_notification = this.resources.getJobNotification()
            .initialise(
                &this.ioCompleted, &this.suspendRequest);

        this.dls_channel.put(timestamp, cast(mstring) value,
                buffer,
                job_notification);

        return true;
    }

    /***************************************************************************

        Called from aio subsystem to resume the request when I/O is available.

    ***************************************************************************/

    private void ioCompleted ()
    {
        this.connection.resumeFiber(NodeResumeCode);
    }

    /***************************************************************************

        Called to suspend the running request.

    ***************************************************************************/

    private void suspendRequest ()
    {
        auto ret = this.connection.suspendFiber();

        if (ret != NodeResumeCode)
        {
            this.connection.event_dispatcher.shutdownWithProtocolError(
                    "No client message is expected doing Put request");
        }
    }
}
