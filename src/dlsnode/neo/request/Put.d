/******************************************************************************

    Put request implementation.

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.neo.request.Put;

import dlsproto.node.neo.request.Put;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;
import dlsproto.common.RequestCodes;
import dlsnode.connection.neo.SharedResources;

import ocean.core.TypeConvert: downcast;

/*******************************************************************************

    Node implementation of the PutProtocol_v1.

*******************************************************************************/

scope class PutImpl_v1: PutProtocol_v1
{
    import swarm.util.Hash;
    import dlsnode.storage.StorageChannels;
    import dlsnode.storage.StorageEngine;
    import core.stdc.time;
    import ocean.meta.types.Qualifiers : cstring, mstring;

    /// Request code/version (required by ConnectionHandler)
    static immutable Command command = Command(RequestCode.Put, 1);

    /// Request name for stats tracking (required by ConnectionHandler)
    static immutable string name = "Put";

    /// Flag indicating whether timing stats should be gathered for requests
    /// of this type
    static immutable bool timing = true;

    /// Flag indicating whether this request type is scheduled for removal
    /// (if `true`, clients will be warned)
    static immutable bool scheduled_for_removal = false;

    /***************************************************************************

        Fiber resume code when resumed from IO system.

    ***************************************************************************/

    static immutable NodeResumeCode = 1;

    /***************************************************************************

        StorageChannels instance.

    ***************************************************************************/

    private StorageChannels storage_channels;

    /***************************************************************************

        StorageChannel instance

    ***************************************************************************/

    private StorageEngine dls_channel;

    /***************************************************************************

        Create/get the channel to put the record to.

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.storage_channels =
            downcast!(SharedResources.RequestResources)(this.resources).storage_channels;
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

        auto job_notification =
            downcast!(SharedResources.RequestResources)(this.resources).getJobNotification()
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
