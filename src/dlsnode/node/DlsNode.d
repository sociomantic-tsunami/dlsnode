/*******************************************************************************

    DLS node implementation

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.node.DlsNode;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.node.model.NeoChannelsNode : ChannelsNodeBase;

import ocean.transition;
import ocean.core.TypeConvert;

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
    import NeoSharedResources = dlsnode.connection.neo.SharedResources;
    import dlsnode.connection.SharedResources;
    import swarm.node.model.NeoNode;

    import dlsnode.storage.StorageChannels;

    import ocean.io.select.EpollSelectDispatcher;

    import ocean.io.compress.lzo.LzoChunkCompressor;

    import swarm.neo.authentication.HmacDef: Key;
    import dlsnode.neo.RequestHandlers;


    /***************************************************************************

        DLS node state

    ***************************************************************************/

    private State state_;


    /**************************************************************************

        PCRE complexity limit.

    ***************************************************************************/

    private static immutable PcreComplexityLimit = 10_000;


    /***************************************************************************

        Constructor.

        Params:
            node_item = node address/ legacy port
            neo_port = port for node to listen for neo requests
            channels = storage channels instance to use
            epoll = epoll select dispatcher to be used internally
            backlog = (see ISelectListener ctor)
            per_request_stats = names of requests to be stats tracked
            no_delay = indicator if the no_delay should be used
            unix_socket_path = path where unix socket should bind to
            credentials_path = path to the neo credentails file
            async_io = AsyncIO instance
            file_buffer_size = size of the input file buffer.

    ***************************************************************************/

    public this ( NodeItem node_item,
        ushort neo_port,
        StorageChannels channels,
        EpollSelectDispatcher epoll,
        int backlog, istring[] per_request_stats,
        bool no_delay,
        istring unix_socket_path,
        istring credentials_path,
        AsyncIO async_io,
        size_t file_buffer_size )
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

        Options neo_options;
        neo_options.requests = requests;
        neo_options.epoll = epoll;
        neo_options.shared_resources =
            new NeoSharedResources.SharedResources(channels,
                async_io, file_buffer_size);
        neo_options.unix_socket_path = unix_socket_path;
        neo_options.credentials_filename = credentials_path;


        super(node_item, neo_port, channels, conn_setup_params,
                neo_options, backlog);

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

    /**************************************************************************

        Calls `callback` with a `RequestResources` object whose scope is limited
        to the run-time of `callback`.

        Params:
            callback = a callback to call with a `RequestResources` object.

    **************************************************************************/

    override protected void getResourceAcquirer (
            scope void delegate ( Object request_resources ) callback )
    {
        auto node_resources = downcast!(NeoSharedResources.SharedResources)(this.shared_resources);
        scope request_resources = node_resources.new RequestResources;
        callback(request_resources);
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
    static immutable len = 29;

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
