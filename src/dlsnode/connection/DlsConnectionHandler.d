/*******************************************************************************

    DLS Node Connection Handler

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.connection.DlsConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.node.connection.ConnectionHandler;

import swarm.node.model.INodeInfo;

import dlsproto.client.legacy.DlsConst;

import dlsnode.node.IDlsNodeInfo;

import dlsnode.connection.SharedResources;

import dlsnode.request.model.RequestResources;

import dlsnode.request.GetVersionRequest;
import dlsnode.request.GetChannelsRequest;
import dlsnode.request.GetSizeRequest;
import dlsnode.request.GetChannelSizeRequest;
import dlsnode.request.GetAllRequest;
import dlsnode.request.GetAllFilterRequest;
import dlsnode.request.RemoveChannelRequest;
import dlsnode.request.GetNumConnectionsRequest;
import dlsnode.request.PutRequest;
import dlsnode.request.GetRangeRequest;
import dlsnode.request.GetRangeFilterRequest;
import dlsnode.request.GetRangeRegexRequest;
import dlsnode.request.RedistributeRequest;
import dlsnode.request.PutBatchRequest;

import dlsproto.node.request.model.DlsCommand;
import dlsnode.util.aio.AsyncIO;

/*******************************************************************************

    DLS node connection handler setup class.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionSetupParams

*******************************************************************************/

public class DlsConnectionSetupParams : ConnectionSetupParams
{
    import dlsnode.storage.StorageChannels;
    import dlsnode.connection.SharedResources;

    import ocean.io.compress.lzo.LzoChunkCompressor;


    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;


    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    public SharedResources shared_resources;


    /***************************************************************************

        Lzo de/compressor.

    ***************************************************************************/

    public LzoChunkCompressor lzo;


    /***************************************************************************

        Reference to the PCRE object shared between all connection handlers.
        This is required by the GetRangeRegex request

    ***************************************************************************/

    public PCRE pcre;


    /**************************************************************************

        AsyncIO instance

    **************************************************************************/

    public AsyncIO async_io;
}


/*******************************************************************************

    DLS node connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside KVNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class DlsConnectionHandler
    : ConnectionHandlerTemplate!(DlsConst.Command)
{
    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to KVConnectionHandler's
        constructor (in the KVConnectionSetupParams instance). Acquired
        resources are automatically relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class DlsRequestResources
        : RequestResources, IDlsRequestResources
    {
        import ocean.io.compress.lzo.LzoChunkCompressor;
        import ocean.io.select.fiber.SelectFiber;


        /**********************************************************************

            Forward methods of DlsCommand.Resources to own implementations

        **********************************************************************/

        override public mstring* getChannelBuffer ( )
        {
            return this.channel_buffer();
        }

        /// ditto
        override public mstring* getKeyBuffer ( )
        {
            return this.key_buffer();
        }

        /// ditto
        override public mstring* getKeyUpperBuffer ( )
        {
            return this.key2_buffer();
        }

        /// ditto
        override public mstring* getFilterBuffer ( )
        {
            return this.filter_buffer();
        }

        /// ditto
        override public mstring* getValueBuffer ( )
        {
            return this.value_buffer();
        }

        /// ditto
        override public ubyte[]* getCompressBuffer ( )
        {
            return cast(ubyte[]*) this.batch_buffer();
        }

        /// ditto
        override public ubyte[]* getPutBatchCompressBuffer ( )
        {
            return this.putbatch_compress_buffer();
        }

        /// ditto
        override public RecordBatcher getRecordBatcher ( )
        {
            return this.batcher();
        }

        /// ditto
        override public RecordBatch getDecompressRecordBatch ( )
        {
            return this.decompress_record_batch;
        }

        /// ditto
        override public NodeItem[]* getRedistributeNodeBuffer ( )
        {
            return this.redistribute_node_buffer();
        }


        /***********************************************************************

            Constructor.

        ***********************************************************************/

        public this ( )
        {
            super(this.setup.shared_resources);
        }


        /***********************************************************************

            Storage channels getter.

        ***********************************************************************/

        override public StorageChannels storage_channels ( )
        {
            return this.setup.storage_channels;
        }


        /***********************************************************************

            Node info getter.

        ***********************************************************************/

        override public IDlsNodeInfo node_info ( )
        {
            return cast(IDlsNodeInfo)this.setup.node_info;
        }


        /***********************************************************************

            PCRE instance getter.

        ***********************************************************************/

        public PCRE pcre ( )
        {
            return this.setup.pcre;
        }

        /**************************************************************************

            AsyncIO instance getter.

        **************************************************************************/

        AsyncIO async_io ( )
        {
            return this.setup.async_io;
        }


        /***********************************************************************

            Channel buffer newer.

        ***********************************************************************/

        override protected mstring new_channel_buffer ( )
        {
            return new char[32];
        }


        /***********************************************************************

            Key buffer newers.

        ***********************************************************************/

        override protected mstring new_key_buffer ( )
        {
            return new char[16]; // 16 hex digits in a 64-bit hash
        }

        override protected mstring new_key2_buffer ( )
        {
            return new char[16]; // 16 hex digits in a 64-bit hash
        }


        /***********************************************************************

            Value buffer newer.

        ***********************************************************************/

        override protected mstring new_value_buffer ( )
        {
            return new char[512];
        }


        /***********************************************************************

            Channel list buffer newer

        ***********************************************************************/

        protected override cstring[] new_channel_list_buffer ( )
        {
            return new cstring[this.storage_channels.length];
        }


        /***********************************************************************

            Filter buffer newer.

        ***********************************************************************/

        override protected mstring new_filter_buffer ( )
        {
            return new char[10];
        }


        /***********************************************************************

            Batch buffer newer.

        ***********************************************************************/

        override protected mstring new_batch_buffer ( )
        {
            return new char[RecordBatcher.DefaultMaxBatchSize];
        }


        /***********************************************************************

            Record buffer newer.

        ***********************************************************************/

        override protected ubyte[] new_record_buffer ( )
        {
            // size of value buffer + key buffer + checksum
            return new ubyte[512 + 16 + 1];
        }


        /***********************************************************************

            PutBatch compress buffer newer.

        ***********************************************************************/

        override protected ubyte[] new_putbatch_compress_buffer ( )
        {
            return new ubyte[DlsConst.PutBatchSize];
        }

        /***********************************************************************

            Hash buffer newer.

        ***********************************************************************/

        override protected hash_t[] new_hash_buffer ( )
        {
            return new hash_t[10];
        }
        
        /***********************************************************************

            Bucket path buffer newer.

        ***********************************************************************/

        override protected mstring new_bucket_path_buffer ( )
        {
            return new char[64];
        }


        /***********************************************************************

            Select event newer.

        ***********************************************************************/

        override protected FiberSelectEvent new_event ( )
        {
            return new FiberSelectEvent(this.outer.fiber);
        }

        /***********************************************************************

            EventFDJobNotification newer.

        ***********************************************************************/

        override protected EventFDJobNotification
             new_suspended_job ( )
        {
            return new EventFDJobNotification(
                    new FiberSelectEvent(this.outer.fiber));
        }

        /***********************************************************************

            Step iterator newer.

        ***********************************************************************/

        override protected StorageEngineStepIterator new_iterator ( )
        {
            return this.storage_channels.newIterator();
        }


        /***********************************************************************

            Bucket iterator newer.

        ***********************************************************************/

        override protected StorageEngineFileIterator new_bucket_iterator ( )
        {
            return this.storage_channels.newFileIterator();
        }


        /***********************************************************************

            Loop ceder newer.

        ***********************************************************************/

        override protected LoopCeder new_loop_ceder ( )
        {
            return new LoopCeder(this.event);
        }


        /***********************************************************************

            Record batcher newer.

        ***********************************************************************/

        override protected RecordBatcher new_batcher ( )
        {
            return new RecordBatcher(this.setup.lzo.lzo);
        }


        /***********************************************************************

            Record batch newer.

        ***********************************************************************/

        override protected RecordBatch new_record_batch ( )
        {
            return new RecordBatch(this.setup.lzo.lzo);
        }

        /***********************************************************************

            Record PutBatch batch newer.

        ***********************************************************************/

        override protected RecordBatch new_decompress_record_batch ( )
        {
            return new RecordBatch(this.setup.lzo.lzo);
        }

        /***********************************************************************

            PCRE regex newer.

        ***********************************************************************/

        override protected CompiledRegex new_regex ( )
        {
            return this.pcre().new CompiledRegex;
        }


        /***********************************************************************

            Redistribute Node buffer newer.

        ***********************************************************************/

        override protected NodeItem[] new_redistribute_node_buffer ( )
        {
            return new NodeItem[6];
        }


        /***********************************************************************

            DlsClient newer.

        ***********************************************************************/

        override protected DlsClient new_dls_client ( )
        {
            return new DlsClient(this.outer.fiber.epoll);
        }


        /***********************************************************************

            Select event initialiser.

        ***********************************************************************/

        override protected void init_event ( FiberSelectEvent event )
        {
            event.fiber = this.outer.fiber;
        }

        /***********************************************************************

            EventFDJobNotification initialiser.

        ***********************************************************************/

        override protected void init_suspended_job (
                EventFDJobNotification req )
        {
            req.setFiber(this.outer.fiber);
        }


        /***********************************************************************

            Loop ceder initialiser.

        ***********************************************************************/

        override protected void init_loop_ceder ( LoopCeder loop_ceder )
        {
            loop_ceder.event = this.event;
        }


        /***********************************************************************

            Internal connection setup params getter.

        ***********************************************************************/

        private DlsConnectionSetupParams setup ( )
        {
            return cast(DlsConnectionSetupParams)this.outer.setup;
        }
    }


    /***************************************************************************

        Reuseable exception thrown when the command code read from the client
        is not supported (i.e. does not have a corresponding entry in
        this.requests).

    ***************************************************************************/

    private Exception invalid_command_exception;


    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing setup data for this connection

    ***************************************************************************/

    public this ( scope FinalizeDg finalize_dg, ConnectionSetupParams setup )
    {
        super(finalize_dg, setup);

        this.invalid_command_exception = new Exception("Invalid command",
            __FILE__, __LINE__);
    }


    /***************************************************************************

        Command code 'None' handler. Treated the same as an invalid command
        code.

    ***************************************************************************/

    override protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }


    /***************************************************************************

        Command code 'GetVersion' handler.

    ***************************************************************************/

    override protected void handleGetVersion ( )
    {
        this.handleCommand!(GetVersionRequest);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnectionsRequest);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannelsRequest);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        this.handleCommand!(GetSizeRequest);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSizeRequest);
    }


    /***************************************************************************

        Command code 'Put' handler.

    ***************************************************************************/

    override protected void handlePut ( )
    {
        this.handleCommand!(PutRequest, RequestStatsTracking.TimeAndCount);
    }


    /***************************************************************************

        Command code 'GetAll' handler.

    ***************************************************************************/

    override protected void handleGetAll ( )
    {
        this.handleCommand!(GetAllRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'GetAllFilter' handler.

    ***************************************************************************/

    override protected void handleGetAllFilter ( )
    {
        this.handleCommand!(GetAllFilterRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'GetRange' handler.

    ***************************************************************************/

    override protected void handleGetRange ( )
    {
        this.handleCommand!(GetRangeRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'GetRangeFilter' handler.

    ***************************************************************************/

    override protected void handleGetRangeFilter ( )
    {
        this.handleCommand!(GetRangeFilterRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'GetRangeRegex' handler.

    ***************************************************************************/

    override protected void handleGetRangeRegex ( )
    {
        this.handleCommand!(GetRangeRegexRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannelRequest);
    }

    /***************************************************************************

        Command code 'Redistribute' handler

    ***************************************************************************/

    override protected void handleRedistribute ( )
    {
        this.handleCommand!(RedistributeRequest);
    }

    /***************************************************************************

        Command code 'PutBatch' handler.

    ***************************************************************************/

    override protected void handlePutBatch ( )
    {
        this.handleCommand!(PutBatchRequest);
    }


    /***************************************************************************

        Command handler method template.

        Template params:
            Handler = type of request handler
            stats_tracking = request stats tracking mode (see enum in
                swarm.node.connection.ConnectionHandler)

    ***************************************************************************/

    private void handleCommand ( Handler : DlsCommand,
            RequestStatsTracking stats_tracking = RequestStatsTracking.None ) ( )
    {
        scope resources = new DlsRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);

        this.handleRequest!(ConnectionResources, DlsRequestResources,
            stats_tracking)(handler, resources, handler.command_name);
    }
}

