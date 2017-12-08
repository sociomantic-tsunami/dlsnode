/******************************************************************************

    GetRange request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.neo.request.GetRange;

import dlsproto.node.neo.request.GetRange;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;
import dlsproto.common.RequestCodes;
import dlsnode.connection.neo.SharedResources;
import ocean.text.regex.PCRE;
import ocean.text.Search;

import ocean.core.Array;
import ocean.transition;
import ocean.core.TypeConvert: downcast;

import dlsnode.util.aio.DelegateJobNotification;

/*******************************************************************************

    Static module logger

*******************************************************************************/

import ocean.util.log.Logger;
private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.request.neo.GetRange");
}

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the GetRange command as specified by
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
            scope rq = new GetRangeImpl_v0(dls_shared_resources.storage_channels, rq_resources);
            rq.handle(connection, msg_payload);
            break;

        case 1:
            scope rq_resources = dls_shared_resources.new RequestResources;
            scope rq = new GetRangeImpl_v1(dls_shared_resources.storage_channels, rq_resources);
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

    Node implementation of the GetRangeProtocol_v0.

*******************************************************************************/

private scope class GetRangeImpl_v0: GetRangeProtocol_v0
{
    import swarm.util.Hash;
    import dlsnode.storage.StorageChannels;
    import dlsnode.storage.StorageEngine;
    import dlsnode.storage.iterator.NeoStorageEngineStepIterator;
    import core.stdc.time;

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

        The lower and upper bound of keys to include in the iteration. Keys
        outside of this range will be rejected (see getNext(), below).

    ***************************************************************************/

    private time_t key_lower, key_higher;

    /***************************************************************************

        Set to iterator over requested channel.

    ***************************************************************************/

    private NeoStorageEngineStepIterator iterator;

    /***************************************************************************

        PCRE engine instance

    ***************************************************************************/

    private PCRE.CompiledRegex regex;

    /***************************************************************************

        Search instance

    ***************************************************************************/

    private SearchFruct!(Const!(char)) match;

    /***************************************************************************

        Filter mode to use for filtering the results.

    ***************************************************************************/

    private Filter.FilterMode filter_mode;

    /***************************************************************************

        Filter string to use for filtering the results.

    ***************************************************************************/

    private cstring filter_string;

    /***************************************************************************

        JobNotification instance for resuming the request.

    ***************************************************************************/

    private DelegateJobNotification job_notification;

    /***************************************************************************

        Constructor.

        Params:
            storage_channels = storage channels resource
            resources = resources to use during the request

    ***************************************************************************/

    public this ( StorageChannels storage_channels,
            SharedResources.RequestResources resources )
    {
        super(resources);
        this.storage_channels = storage_channels;
        this.resources = resources;
        this.job_notification = this.resources.getJobNotification()
            .initialise(&this.dataReady);
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

        Performs any logic needed to start reading records in the given
        range.

        Params:
            low = lower range boundary
            high = higher range boundary

        Returns:
            `true` if the range preparation was successful

    ***************************************************************************/

    override protected bool prepareRange ( time_t low, time_t high )
    {
        assert(this.dls_channel);

        this.key_lower = low;
        this.key_higher = high;

        if (this.key_lower > this.key_higher)
        {
            return false;
        }

        this.iterator = this.resources.getNeoStepIterator();

        this.iterator.getRange(this.dls_channel,
                this.key_lower, this.key_higher);

        return true;
    }

    /***************************************************************************

        Performs the filtering on the record, based on the filter mode and
        time range.

        Params:
            timestamp = timestamp of the record
            value = value of the record

        Returns:
            true if the record should be forwarded to the client,
            false otherwise.

    ***************************************************************************/

    private bool filterRecord (time_t key, void[] value)
    {
        if (key < this.key_lower || key > this.key_higher)
        {
            return false;
        }

        with ( Filter.FilterMode ) switch ( this.filter_mode )
        {
            case None:
                return true;

            case StringMatch:
                return this.match.forward(cast(mstring)value) < value.length;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    return this.regex.match(cast(mstring)value);
                }
                catch ( .PCRE.PcreException e )
                {
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and "
                        "key '{}': {} (error code: {}) @ {}:{} (aborting iteration)",
                        this.filter_string, this.dls_channel.id, key,
                        e.msg, e.error, e.file, e.line);
                    return false;
                }
                catch ( Exception e )
                {
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and "
                        "key '{}': {} @ {}:{} (aborting iteration)",
                        this.filter_string, this.dls_channel.id,
                        key, e.msg, e.file, e.line);
                    return false;
                }
                assert(false);

            default:
                assert(false);
        }

        assert(false);
    }

    /***************************************************************************

        Retrieve the next record from the channel, if available.

        Params:
            timestamp = variable to write the record's timestamp into
            value = buffer to write the value into
            wait_for_data = indicator if the request handler should suspend and
                            wait for the storage engine to finish, and request
                            this again

        Returns:
            `true` if there was a record in the channel, false if the channel is
            empty

    ***************************************************************************/

    override protected bool getNextRecord ( out time_t timestamp, ref void[] value,
            out bool wait_for_data)
    {
        for (;;)
        {
            bool last_key = this.iterator.next(this.job_notification, timestamp, value,
                    wait_for_data);

            if (wait_for_data)
            {
                return true;
            }

            if (last_key)
            {
                return false;
            }

            if (!this.filterRecord(timestamp, value))
            {
                continue;
            }

            return true;
        }

        assert(false);
    }

    /***************************************************************************

        Allows request to process read filter string into more efficient form
        and save it before starting actual record iteration.

        Params:
            mode = filter mode
            filter = filter string

        Returns:
            true if preparing filter is successful, false otherwise

    ***************************************************************************/

    override protected bool prepareFilter ( Filter.FilterMode mode,
        cstring filter )
    {
        this.filter_mode = mode;
        this.filter_string = filter;

        with (Filter.FilterMode) switch (mode)
        {
            case StringMatch:
                this.match = search(filter);
                break;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    this.regex = this.resources.getCompiledRegex();
                    auto case_sens = mode != PCRECaseInsensitive;
                    this.regex.compile(filter, case_sens);
                }
                catch ( .PCRE.PcreException e )
                {
                    .log.error("Exception during PCRE compile of `{}` on channel '{}': "
                        "{} (error code: {}) @ {}:{} (aborting iteration)",
                        filter, this.dls_channel.id,
                        e.toString(), e.error, e.file, e.line);
                    return false;
                }
                catch ( Exception e )
                {
                    .log.error("Exception during PCRE compile of `{}` on channel '{}': "
                        "{} @ {}:{} (aborting iteration)",
                        filter, this.dls_channel.id,
                        e.toString(), e.file, e.line);
                    return false;
                }
                break;

            default:
                assert(false);
        }

        return true;
    }

    /***************************************************************************

        Indicates storage engine that this request has been suspended and that
        results are no longer required.

    ***************************************************************************/

    override protected void requestSuspended ()
    {
        this.job_notification.discardResults();
    }

    /***************************************************************************

        Indicates storage engine that this request has been stopped and that
        results are no longer required.

    ***************************************************************************/

    override protected void requestFinished ()
    {
        this.job_notification.discardResults();
    }
}

/*******************************************************************************

    Node implementation of the GetRangeProtocol_v1.

*******************************************************************************/

private scope class GetRangeImpl_v1: GetRangeProtocol_v1
{
    import swarm.util.Hash;
    import dlsnode.storage.StorageChannels;
    import dlsnode.storage.StorageEngine;
    import dlsnode.storage.iterator.NeoStorageEngineStepIterator;
    import core.stdc.time;

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

        The lower and upper bound of keys to include in the iteration. Keys
        outside of this range will be rejected (see getNext(), below).

    ***************************************************************************/

    private time_t key_lower, key_higher;

    /***************************************************************************

        Set to iterator over requested channel.

    ***************************************************************************/

    private NeoStorageEngineStepIterator iterator;

    /***************************************************************************

        PCRE engine instance

    ***************************************************************************/

    private PCRE.CompiledRegex regex;

    /***************************************************************************

        Search instance

    ***************************************************************************/

    private SearchFruct!(Const!(char)) match;

    /***************************************************************************

        Filter mode to use for filtering the results.

    ***************************************************************************/

    private Filter.FilterMode filter_mode;

    /***************************************************************************

        Filter string to use for filtering the results.

    ***************************************************************************/

    private cstring filter_string;

    /***************************************************************************

        JobNotification instance for resuming the request.

    ***************************************************************************/

    private DelegateJobNotification job_notification;

    /***************************************************************************

        Constructor.

        Params:
            storage_channels = storage channels resource
            resources = resources to use during the request

    ***************************************************************************/

    public this ( StorageChannels storage_channels,
            SharedResources.RequestResources resources )
    {
        super(resources);
        this.storage_channels = storage_channels;
        this.resources = resources;
        this.job_notification = this.resources.getJobNotification()
            .initialise(&this.dataReady);
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

        Performs any logic needed to start reading records in the given
        range.

        Params:
            low = lower range boundary
            high = higher range boundary

        Returns:
            `true` if the range preparation was successful

    ***************************************************************************/

    override protected bool prepareRange ( time_t low, time_t high )
    {
        assert(this.dls_channel);

        this.key_lower = low;
        this.key_higher = high;

        if (this.key_lower > this.key_higher)
        {
            return false;
        }

        this.iterator = this.resources.getNeoStepIterator();

        this.iterator.getRange(this.dls_channel,
                this.key_lower, this.key_higher);

        return true;
    }

    /***************************************************************************

        Performs the filtering on the record, based on the filter mode and
        time range.

        Params:
            timestamp = timestamp of the record
            value = value of the record

        Returns:
            true if the record should be forwarded to the client,
            false otherwise.

    ***************************************************************************/

    private bool filterRecord (time_t key, void[] value)
    {
        if (key < this.key_lower || key > this.key_higher)
        {
            return false;
        }

        with ( Filter.FilterMode ) switch ( this.filter_mode )
        {
            case None:
                return true;

            case StringMatch:
                return this.match.forward(cast(mstring)value) < value.length;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    return this.regex.match(cast(mstring)value);
                }
                catch ( .PCRE.PcreException e )
                {
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and "
                        "key '{}': {} (error code: {}) @ {}:{} (aborting iteration)",
                        this.filter_string, this.dls_channel.id, key,
                        e.msg, e.error, e.file, e.line);
                    return false;
                }
                catch ( Exception e )
                {
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and "
                        "key '{}': {} @ {}:{} (aborting iteration)",
                        this.filter_string, this.dls_channel.id,
                        key, e.msg, e.file, e.line);
                    return false;
                }
                assert(false);

            default:
                assert(false);
        }

        assert(false);
    }

    /***************************************************************************

        Retrieve the next record from the channel, if available.

        Params:
            timestamp = variable to write the record's timestamp into
            value = buffer to write the value into
            wait_for_data = indicator if the request handler should suspend and
                            wait for the storage engine to finish, and request
                            this again

        Returns:
            `true` if there was a record in the channel, false if the channel is
            empty

    ***************************************************************************/

    override protected bool getNextRecord ( out time_t timestamp, ref void[] value,
            out bool wait_for_data)
    {
        for (;;)
        {
            bool last_key = this.iterator.next(this.job_notification, timestamp, value,
                    wait_for_data);

            if (wait_for_data)
            {
                return true;
            }

            if (last_key)
            {
                return false;
            }

            if (!this.filterRecord(timestamp, value))
            {
                continue;
            }

            return true;
        }

        assert(false);
    }

    /***************************************************************************

        Allows request to process read filter string into more efficient form
        and save it before starting actual record iteration.

        Params:
            mode = filter mode
            filter = filter string

        Returns:
            true if preparing filter is successful, false otherwise

    ***************************************************************************/

    override protected bool prepareFilter ( Filter.FilterMode mode,
        cstring filter )
    {
        this.filter_mode = mode;
        this.filter_string = filter;

        with (Filter.FilterMode) switch (mode)
        {
            case StringMatch:
                this.match = search(filter);
                break;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    this.regex = this.resources.getCompiledRegex();
                    auto case_sens = mode != PCRECaseInsensitive;
                    this.regex.compile(filter, case_sens);
                }
                catch ( .PCRE.PcreException e )
                {
                    .log.error("Exception during PCRE compile of `{}` on channel '{}': "
                        "{} (error code: {}) @ {}:{} (aborting iteration)",
                        filter, this.dls_channel.id,
                        e.toString(), e.error, e.file, e.line);
                    return false;
                }
                catch ( Exception e )
                {
                    .log.error("Exception during PCRE compile of `{}` on channel '{}': "
                        "{} @ {}:{} (aborting iteration)",
                        filter, this.dls_channel.id,
                        e.toString(), e.file, e.line);
                    return false;
                }
                break;

            default:
                assert(false);
        }

        return true;
    }

    /***************************************************************************

        Indicates storage engine that this request has been stopped and that
        results are no longer required.

    ***************************************************************************/

    override protected void requestFinished ()
    {
        this.job_notification.discardResults();
    }
}
