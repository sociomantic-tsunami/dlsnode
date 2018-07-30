/*******************************************************************************

    DLS node shared resource manager. Handles acquiring / relinquishing of
    global resources by active request handlers.

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.connection.neo.SharedResources;

import ocean.transition;

/*******************************************************************************

    Resources owned by the node which are needed by the request handlers.

*******************************************************************************/

public final class SharedResources
{
    import ocean.io.compress.Lzo;
    import ocean.util.container.pool.FreeList;
    import swarm.neo.util.AcquiredResources;
    import swarm.util.RecordBatcher;
    import dlsnode.storage.StorageChannels;
    import dlsproto.node.neo.request.core.IRequestResources;
    import dlsnode.storage.iterator.NeoStorageEngineStepIterator;
    import dlsnode.util.aio.AsyncIO;
    import dlsnode.util.aio.DelegateJobNotification;
    import ocean.text.regex.PCRE;
    import ocean.sys.ErrnoException;

    /***************************************************************************

        Pool of buffers to store record values in. (We store ubyte[] buffers
        internally, as a workaround for ambiguites in ocean.core.Buffer because
        void[][] can't implicitly cast to void[].)

    ***************************************************************************/

    private FreeList!(ubyte[]) buffers;

    /***************************************************************************

        Pool of RecordBatch instances to use.

    ***************************************************************************/

    private FreeList!(RecordBatcher) record_batchers;

    /***************************************************************************

        Pool of StorageEngineStepIterator instances to use.

    ***************************************************************************/

    private FreeList!(NeoStorageEngineStepIterator) neo_step_iterators;

    /***************************************************************************

        Pool of DelegateJobNotification instances to use.

    ***************************************************************************/

    private FreeList!(DelegateJobNotification) job_notifications;

    /***************************************************************************

        Pool of reusable exception instances to use.

    ***************************************************************************/

    private FreeList!(Exception) exceptions;

    /***************************************************************************

        PCRE object to obtain compiled regexes from.

    ***************************************************************************/

    private PCRE pcre;

    /***************************************************************************

        Lzo object for compressing the record batches.

    ***************************************************************************/

    private Lzo lzo;

    /***************************************************************************

        Pool of CompiledRegex instances to use.

    ***************************************************************************/

    private FreeList!(PCRE.CompiledRegex) regexes;

    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;

    /***************************************************************************

        Size of the buffer used for file input.

    ***************************************************************************/

    public size_t file_buffer_size;

    /***************************************************************************

        AsyncIO instance.

    ***************************************************************************/

    public AsyncIO async_io;

    /***************************************************************************

        Constructor

        Params:
            storage_channels = StorageChannels instance which the requests are
                               operating on.
            async_io = AsyncIO instance
            file_buffer_size = size of the buffer used for the reading from the
            bucket files.

    ***************************************************************************/

    public this ( StorageChannels storage_channels, AsyncIO async_io,
            size_t file_buffer_size )
    {
        this.pcre = new PCRE;
        this.lzo = new Lzo;
        this.file_buffer_size = file_buffer_size;
        this.async_io = async_io;
        this.storage_channels = storage_channels;

        this.buffers = new FreeList!(ubyte[]);
        this.record_batchers = new FreeList!(RecordBatcher);
        this.neo_step_iterators = new FreeList!(NeoStorageEngineStepIterator);
        this.job_notifications = new FreeList!(DelegateJobNotification);
        this.exceptions = new FreeList!(Exception);
        this.regexes = new FreeList!(PCRE.CompiledRegex);
    }

    /***************************************************************************

        Scope class which may be newed inside request handlers to get access to
        the shared pools of resources. Any acquired resources are relinquished
        in the destructor.

        The class should always be newed as scope, but cannot be declared as
        such because the request handler classes need to store a reference to it
        as a member, which is disallowed for scope instances.

    ***************************************************************************/

    public /*scope*/ class RequestResources: IRequestResources
    {
        /***********************************************************************

            Acquired void arrays.

        ***********************************************************************/

        private AcquiredArraysOf!(void) acquired_void_buffers;

        /***********************************************************************

            Acquired RecordBatchers.

        ***********************************************************************/

        private Acquired!(RecordBatcher) acquired_record_batchers;

        /***********************************************************************

            Acquired StorageEngineStepIterator iterators.

        ***********************************************************************/

        private Acquired!(NeoStorageEngineStepIterator) acquired_neo_step_iterators;

        /***********************************************************************

            Acquired DelegateJobNotification instances.

        ***********************************************************************/

        private Acquired!(DelegateJobNotification) acquired_job_notifications;

        /***********************************************************************

            Acquired reusable exception instances.

        ***********************************************************************/

        private Acquired!(Exception) acquired_exceptions;

        /***********************************************************************

            Acquired reusable CompiledRegex instances.

        ***********************************************************************/

        private Acquired!(PCRE.CompiledRegex) acquired_regexes;

        /***********************************************************************

            Constructor

        ***********************************************************************/

        public this ( )
        {
            this.acquired_void_buffers.initialise(this.outer.buffers);
            this.acquired_record_batchers.initialise(this.outer.buffers,
                    this.outer.record_batchers);
            this.acquired_neo_step_iterators.initialise(this.outer.buffers,
                    this.outer.neo_step_iterators);
            this.acquired_job_notifications.initialise(this.outer.buffers,
                    this.outer.job_notifications);
            this.acquired_exceptions.initialise(this.outer.buffers,
                    this.outer.exceptions);
            this.acquired_regexes.initialise(this.outer.buffers,
                    this.outer.regexes);
        }

        /***********************************************************************

            Destructor. Relinquishes any acquired resources back to the shared
            resources pools.

        ***********************************************************************/

        ~this ( )
        {
            this.acquired_void_buffers.relinquishAll();
            this.acquired_record_batchers.relinquishAll();
            this.acquired_neo_step_iterators.relinquishAll();
            this.acquired_job_notifications.relinquishAll();
            this.acquired_exceptions.relinquishAll();
            this.acquired_regexes.relinquishAll();
        }

        /***********************************************************************

            Returns:
                a pointer to a new chunk of memory (a void[]) to use during
                the request's lifetime.

        ***********************************************************************/

        override public void[]* getVoidBuffer ( )
        {
            return this.acquired_void_buffers.acquire();
        }


        /***********************************************************************

            Returns:
                lzo object for compressing the record batches.

        ***********************************************************************/

        override public Lzo getLzo ( )
        {
            return this.outer.lzo;
        }

        /***********************************************************************

            Returns:
                A RecordBatcher instance to use during
                the request's lifetime.

        ***********************************************************************/

        override public RecordBatcher getRecordBatcher ( )
        {
            return this.acquired_record_batchers.acquire(new RecordBatcher(new Lzo));
        }

        /***********************************************************************

            Returns:
                StorageEngineStepIterator instance to use during the request's
                lifetime.

        ***********************************************************************/

        public NeoStorageEngineStepIterator getNeoStepIterator ( )
        {
            return this.acquired_neo_step_iterators.acquire(
                    new NeoStorageEngineStepIterator(this.outer.async_io,
                                                  this.outer.file_buffer_size));
        }

        /***********************************************************************

            Returns:
                DelegateJobNotification instance to use during the request's
                lifetime.

        ***********************************************************************/

        public DelegateJobNotification getJobNotification ( )
        {
            return this.acquired_job_notifications.acquire(
                    new DelegateJobNotification(null, null));
        }

        /***********************************************************************

            Returns:
                An Exception instance to use during the request's lifetime.

        ***********************************************************************/

        public Exception getException ( )
        {
            return this.acquired_exceptions.acquire(new Exception("", __FILE__, __LINE__));
        }

        /***********************************************************************

            Returns:
                CompiledRegex instance to use during the request's lifetime.

        ***********************************************************************/

        public PCRE.CompiledRegex getCompiledRegex ()
        {
            return this.acquired_regexes.acquire(this.outer.pcre.new CompiledRegex);
        }

        /**********************************************************************

            Returns:
                Reference to StorageChannels instance to use during the
                request's lifetime.

        **********************************************************************/

        public StorageChannels storage_channels ()
        {
            return this.outer.storage_channels;
        }
    }
}
