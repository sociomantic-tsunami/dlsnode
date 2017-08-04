/*******************************************************************************

    DLS node implementation

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.node.DlsNode;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.node.model.ChannelsNode : ChannelsNodeBase;

import ocean.transition;

import dlsnode.node.IDlsNodeInfo;

import dlsnode.storage.StorageChannels;
import dlsnode.storage.StorageEngine;

import dlsnode.connection.DlsConnectionHandler;


import dlsnode.util.aio.AsyncIO;


/*******************************************************************************

    DlsNode

*******************************************************************************/

public class DlsNode :
    ChannelsNodeBase!(StorageEngine, DlsConnectionHandler), IDlsNodeInfo
{
    import swarm.Const : NodeItem;

    import dlsnode.connection.DlsConnectionHandler : DlsConnectionSetupParams;
    import dlsnode.connection.SharedResources;

    import dlsnode.storage.StorageChannels;

    import ocean.io.select.EpollSelectDispatcher;

    import ocean.io.compress.lzo.LzoChunkCompressor;


    /***************************************************************************

        DLS node state

    ***************************************************************************/

    private State state_;


    /**************************************************************************

        PCRE complexity limit.

    ***************************************************************************/

    private const PcreComplexityLimit = 10_000;


    /***************************************************************************

        Constructor.

        Params:
            node_item = node address/port
            channels = storage channels instance to use
            epoll = epoll select dispatcher to be used internally
            backlog = (see ISelectListener ctor)
            per_request_stats = names of requests to be stats tracked
            async_io = AsyncIO instance

    ***************************************************************************/

    public this ( NodeItem node_item, StorageChannels channels,
        EpollSelectDispatcher epoll,
        int backlog, istring[] per_request_stats,
        AsyncIO async_io )
    {

        auto conn_setup_params = new DlsConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = channels;
        conn_setup_params.shared_resources = new SharedResources;
        conn_setup_params.lzo = new LzoChunkCompressor;
        conn_setup_params.pcre = new PCRE;
        conn_setup_params.pcre.complexity_limit = PcreComplexityLimit;
        conn_setup_params.async_io = async_io;

        super(node_item, channels, conn_setup_params, backlog);

        // Initialise requests to be stats tracked.
        foreach ( cmd; per_request_stats )
        {
            this.request_stats.init(cmd);
        }
    }


    /***************************************************************************

        DLS node state setter.

        Params:
            s = new state of node

    ***************************************************************************/

    public void state ( State s )
    {
        this.state_ = s;
    }


    /***************************************************************************

        Returns:
            state of node

    ***************************************************************************/

    override public State state ( )
    {
        return this.state_;
    }


    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    override protected istring id ( )
    {
        return typeof(this).stringof;
    }


    /***************************************************************************

        Returns:
            list of record action counter ids
            (see swarm.node.model.RecordActionCounters)

    ***************************************************************************/

    override protected istring[] record_action_counter_ids ( )
    {
        return ["handled", "redistributed"];
    }
}


version ( UnitTest )
{
    import ocean.core.Test;
    import ocean.text.regex.PCRE;
}


/*******************************************************************************

    Test complexity bail out on a very complex regex.

    From http://manned.org/libpcre.7, a bit about why certain regexes take a
    really long time to resolve:

    ---

        When  a pattern contains an unlimited repeat inside a subpattern that
        can itself be repeated an unlimited number of times, the use of a
        once-only subpattern is the only way to avoid some failing matches
        taking a very long time indeed. The pattern

            (\D+|<\d+>)*[!?]

        matches an unlimited number of substrings that either consist of
        non-digits, or digits enclosed in <>, followed by either ! or ?. When it
        matches, it runs quickly. However, if it is applied to

            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

        it takes a long time before reporting failure. This is because the
        string can be divided between the two repeats in a large number of ways,
        and all have to be tried. (The example used [!?] rather than a single
        character at the end, because both PCRE and Perl have an optimization
        that allows for fast failure when a single character is used. They
        remember the last single character that is required for a match, and
        fail early if it is not present in the string.) If the pattern is
        changed to

            ((?>\D+)|<\d+>)*[!?]

        sequences of non-digits cannot be broken, and failure happens quickly.

    ---

*******************************************************************************/

unittest
{
    const len = 29;

    mstring genString ( )
    {
        mstring str;
        for ( int i; i < len; i++ )
        {
            str ~= "a";
        }
        return str;
    }

    mstring genRegexp ( )
    {
        mstring str;
        for ( int i; i < len; i++ )
        {
            str ~= "a?";
        }
        str ~= ".";
        str ~= genString();
        return str;
    }

    auto pcre = new PCRE;
    pcre.complexity_limit = DlsNode.PcreComplexityLimit;

    testThrown!(PCRE.PcreException)(pcre.preg_match(genString(), genRegexp()));
}
