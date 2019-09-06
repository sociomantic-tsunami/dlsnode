/******************************************************************************

    GetRange request implementation.

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

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

    Node implementation of the GetRangeProtocol_v2

*******************************************************************************/

scope class GetRangeImpl_v2: GetRangeProtocol_v2
{
    import swarm.util.Hash;
    import dlsnode.storage.StorageChannels;
    import dlsnode.storage.StorageEngine;
    import dlsnode.storage.iterator.NeoStorageEngineStepIterator;
    import core.stdc.time;
    import ocean.meta.types.Qualifiers : Const, cstring, istring, mstring;

    /// Request code/version (required by ConnectionHandler)
    static immutable Command command = Command(RequestCode.GetRange, 2);

    /// Request name for stats tracking (required by ConnectionHandler)
    static immutable istring name = "GetRange";

    /// Flag indicating whether timing stats should be gathered for requests
    /// of this type
    static immutable bool timing = true;

    /// Flag indicating whether this request type is scheduled for removal
    /// (if `true`, clients will be warned)
    static immutable bool scheduled_for_removal = false;

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

    private SearchFruct match;

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

        Create/get the channel to put the record to.

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        auto node_resources = downcast!(SharedResources.RequestResources)(this.resources);
        if (auto storage_channel = node_resources.storage_channels.getCreate(channel_name))
        {
            this.dls_channel = downcast!(StorageEngine)(storage_channel);
            assert(this.dls_channel);

            this.job_notification = node_resources.getJobNotification()
                .initialise(&this.dataReady);

            return true;
        }
        else
        {
            return false;
        }
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
        auto node_resources = downcast!(SharedResources.RequestResources)(this.resources);

        this.key_lower = low;
        this.key_higher = high;

        if (this.key_lower > this.key_higher)
        {
            return false;
        }

        this.iterator = node_resources.getNeoStepIterator();

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
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and " ~
                        "key '{}': {} (error code: {}) @ {}:{} (aborting iteration)",
                        this.filter_string, this.dls_channel.id, key,
                        e.msg, e.error, e.file, e.line);
                    return false;
                }
                catch ( Exception e )
                {
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and " ~
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
            auto res = this.iterator.next(this.job_notification, timestamp, value);

            with (NeoStorageEngineStepIterator.NextResult)
            {
                switch (res)
                {
                    case NoMoreData:
                        return false;
                    case WaitForData:
                        wait_for_data = true;
                        return true;
                    case RecordRead:
                        if (!this.filterRecord(timestamp, value))
                        {
                            break;
                        }
                        return true;
                    default:
                        assert(false);
                }
            }
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
        auto node_resources = downcast!(SharedResources.RequestResources)(this.resources);

        with (Filter.FilterMode) switch (mode)
        {
            case StringMatch:
                this.match = search(filter);
                break;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    this.regex = node_resources.getCompiledRegex();
                    auto case_sens = mode != PCRECaseInsensitive;
                    this.regex.compile(filter, case_sens);
                }
                catch ( .PCRE.PcreException e )
                {
                    .log.error("Exception during PCRE compile of `{}` on channel '{}': " ~
                        "{} (error code: {}) @ {}:{} (aborting iteration)",
                        filter, this.dls_channel.id,
                        e.toString(), e.error, e.file, e.line);
                    return false;
                }
                catch ( Exception e )
                {
                    .log.error("Exception during PCRE compile of `{}` on channel '{}': " ~
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
