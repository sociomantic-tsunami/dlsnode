/*******************************************************************************

    DLS Node Server

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.main;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.io.select.client.SelectEvent;
import ocean.util.app.DaemonApp;

// FIXME: this import is static in order to resolve conflict with
// deprecated Log imported via imports in DlsNodeServer. Should be made
// non-static with ocean v4.x.x
static import ocean.util.log.Logger;

import ocean.text.util.StringC;
import ocean.sys.ErrnoException;
import core.stdc.errno;
import core.stdc.stdio;
import core.stdc.stdlib;
import core.stdc.string;
import core.sys.posix.unistd;
import core.sys.posix.sys.stat;
import core.sys.posix.fcntl;
import ocean.transition;

import ocean.transition;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private ocean.util.log.Logger.Logger logger;

static this ( )
{
    .logger = ocean.util.log.Logger.Log.lookup("dlsnode.main");
}


/***************************************************************************

    SelectClient used for forwarding signals to terminate handler.

***************************************************************************/

private SelectEvent terminate_event;


/***************************************************************************

    Signal handler. Redirect the signal to SelectEvent which will be handled
    by epoll.

    Params:
        signum = signal received

***************************************************************************/

private extern(C) void signalHandler (int signum)
{
    .terminate_event.trigger();
}


/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts DLS node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

version (UnitTest) {} else
private int main ( istring[] cl_args )
{
    auto app = new DlsNodeServer;
    return app.main(cl_args);
}



/*******************************************************************************

    DLS node application base class

*******************************************************************************/

public class DlsNodeServer : DaemonApp
{
    import Version;

    import dlsnode.app.config.ServerConfig;
    import dlsnode.app.config.PerformanceConfig;

    import dlsnode.storage.checkpoint.CheckpointService;
    import dlsnode.storage.checkpoint.CheckpointFile;

    import dlsnode.node.DlsNode;
    import dlsnode.connection.DlsConnectionHandler;
    import dlsnode.storage.StorageChannels;
    import dlsnode.storage.BufferedBucketOutput;

    import dlsnode.util.aio.AsyncIO;

    import ocean.core.MessageFiber;

    import ocean.io.device.File;
    import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;
    import ocean.io.select.selector.EpollException;

    import ocean.util.config.ConfigParser;
    import ConfigReader = ocean.util.config.ConfigFiller;

    import ocean.io.select.EpollSelectDispatcher;

    import ocean.io.select.client.model.ISelectClient;

    import ocean.io.Stdout;

    import Hash = ocean.text.convert.Hash;

    import dlsproto.client.legacy.DlsConst;
    import swarm.util.node.log.Stats;

    import ocean.core.ExceptionDefinitions : IOException, OutOfMemoryException;
    import ocean.sys.ErrnoException;

    import core.sys.posix.signal;
    import core.sys.posix.sys.mman : mlockall, MCL_CURRENT, MCL_FUTURE;
    import core.stdc.errno : errno, EPERM, ENOMEM;
    import core.stdc.string : strerror_r;
    import ocean.sys.CpuAffinity;
    import ocean.sys.Stats;


    /***************************************************************************

        DLS node config values

    ***************************************************************************/

    private static class DlsConfig
    {
        size_t write_buffer_size = BufferedBucketOutput.DefaultBufferSize;
    }

    private DlsConfig dls_config;


    /***************************************************************************

        Config classes for server, performance and stats

    ***************************************************************************/

    private ServerConfig server_config;

    private PerformanceConfig performance_config;


    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Key/value node instance. Constructed after the config file has been
        parsed -- currently the type of node is setin the config file.

    ***************************************************************************/

    private DlsNode node;


    /**************************************************************************

        AsyncIO instance

    **************************************************************************/

    private AsyncIO async_io;

    /***************************************************************************

        PID lock fd.

    ***************************************************************************/

    int lock_fd;

    /**************************************************************************

        Checkpoint engine.

    **************************************************************************/

    private CheckpointService checkpointer;


    /***************************************************************************

        Node stats logger.

    ***************************************************************************/

    private NodeStats dls_stats;

    /***************************************************************************

        Constructor

    ***************************************************************************/

    public this ( )
    {
        assert (.terminate_event is null);

        const app_name = "dlsnode";
        const app_desc = "dlsnode: DLS server node.";

        this.epoll = new EpollSelectDispatcher;

        super(app_name, app_desc, version_info);
    }


    /***************************************************************************

        Get values from the configuration file. Overridden to read additional
        DLS config options.

        Params:
            app = application instance
            config = config parser instance

    ***************************************************************************/

    override protected void processConfig ( IApplication app, ConfigParser config )
    {
        super.processConfig(app, config);

        ConfigReader.fill("Server", this.server_config, config);
        ConfigReader.fill("Performance", this.performance_config, config);
        ConfigReader.fill("Options_Dls", this.dls_config, config);
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    protected override int run ( Arguments args, ConfigParser config )
    {
        // Install termination signal handlers
        .terminate_event = new SelectEvent(&this.terminateApp);
        this.installSignalHandlers();

        // Set CPU affinity of the process (all child threads will inherit this,
        // if set)
        auto cpu_affinity = this.server_config.cpu();

        if (cpu_affinity >= 0)
        {
            CpuAffinity.set(cast(uint)cpu_affinity);
        }

        // initialize async io manager
        this.async_io = new AsyncIO(this.epoll,
                this.performance_config.number_of_thread_workers);

        // Truncate buckets to the last checkpointed position
        CheckpointFile.truncateBuckets("data", "checkpoint.dat");

        // Create Checkpoint service
        this.checkpointer = new CheckpointService("data", "checkpoint.dat");

        auto storage_channels = new StorageChannels(
            this.server_config.data_dir,
            this.checkpointer, this.async_io,
            this.performance_config.file_buffer_size,
            this.dls_config.write_buffer_size);

        this.node = new DlsNode(this.node_item,
                this.server_config.neoport(),
                storage_channels, this.epoll,
                server_config.backlog, this.per_request_stats,
                this.performance_config.no_delay,
                idup(this.server_config.unix_socket_path()),
                idup(this.server_config.credentials_path),
                this.async_io,
                this.performance_config.file_buffer_size);

        this.dls_stats =
            new NodeStats(this.node, this.stats_ext.stats_log);

        this.node.error_callback = &this.nodeError;
        this.node.connection_limit = server_config.connection_limit;

        logger.info("Starting checkpoint service ----------------------");

        this.checkpointer.startService(this.epoll,
                this.performance_config.checkpoint_commit_seconds);

        logger.info("Starting DLS node --------------------------------");

        this.startEventHandling(this.epoll);

        this.timer_ext.register(&this.flushData,
            cast(double)this.performance_config.write_flush_ms / 1000.0);

        this.node.register(this.epoll);

        this.epoll.register(.terminate_event);

        logger.info("Starting event loop");
        this.epoll.eventLoop();
        logger.info("Event loop exited");

        this.restoreSignalHandlers();

        return 0;
    }


    /**************************************************************************

        Periodic stats update callback.

    ***************************************************************************/

    override protected void onStatsTimer ( )
    {
        this.reportSystemStats();
        this.dls_stats.log();
        this.stats_ext.stats_log.add(ocean.util.log.Logger.Log.stats());
        this.stats_ext.stats_log.add(getNumFilesStats());
        this.stats_ext.stats_log.flush();
    }


    /***************************************************************************

        Periodic node's data flush callback

        Returns:
            always true to stay registered

    ***************************************************************************/

    private bool flushData ( )
    {
        assert(this.node);
        this.node.flush();
        return true;
    }


    /***************************************************************************

        Returns:
            list of names of requests to be stats tracked

    ***************************************************************************/

    private istring[] per_request_stats ( )
    out ( rqs )
    {
        foreach ( rq; rqs )
        {
            assert(rq in DlsConst.Command(),
                "Cannot track stats for unknown request " ~ rq);
        }
    }
    body
    {
        return ["Put", "GetRange", "GetAll", "GetAllFilter",
                "GetRangeFilter", "Redistribute", "PutBatch",
                "GetRangeRegex"];
    }




    /***************************************************************************

        Returns:
            node item (address/port) for this node

    ***************************************************************************/

    private DlsConst.NodeItem node_item ( )
    {
        return DlsConst.NodeItem(this.server_config.address(), this.server_config.port());
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the log file.

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred
            conn = info about the connection handler where the exception
                occurred

    ***************************************************************************/

    private void nodeError ( Exception exception,
        IAdvancedSelectClient.Event event_info,
        DlsConnectionHandler.IConnectionHandlerInfo conn )
    {
        // FIXME: any errors which occur after the terminateApp() has exited
        // are just printed to the console. This is a hack to work around an
        // unknown compiler bug which causes segfaults inside the tango logger
        // (apparently something to do with variadic args) due to the ptr and
        // length of an array being swapped in the arguments list of a function.
        // We need to investigate this properly and try to work out what the bug
        // is.
        if ( this.node.state == DlsNode.State.ShutDown )
        {
            Stderr.formatln("Node error: " ~ exception.msg);
            return;
        }

        if ( auto e = cast (File.IOException)exception)
        {
            // This is a subclass of IOException, which is
            // thrown by ocean's regular IO methods. This is not something
            // that normally occurs, and we need to log it.
            logger.error("File.IOException caught in eventLoop: '{}' @ {}:{}",
                getMsg(e), e.file, e.line);
        }
        else if ( cast(MessageFiber.KilledException)exception ||
             cast(IOWarning)exception ||
             cast(IOException)exception ||
             cast(EpollException)exception )
        {
            // Don't log these exception types, which only occur on the normal
            // disconnection of a client.
        }
        else
        {
            logger.error("Exception caught in eventLoop: '{}' @ {}:{} on {}:{}",
                getMsg(exception), exception.file, exception.line,
                this.server_config.address(), this.server_config.port());
        }
    }

    /***************************************************************************

        Contains the signal actions at the startup time. Used to restore the
        signal behaviour at the end.

    ***************************************************************************/

    private sigaction_t old_sigint, old_sigterm, old_sigrtmin;

    /***************************************************************************

        Installs the SIGINT, SIGTERM AND SIGRTMIN handlers.

    ***************************************************************************/

    private void installSignalHandlers ()
    {
        sigaction_t sa;

        sa.sa_handler = &.signalHandler;

        if (sigemptyset(&sa.sa_mask) == -1)
        {
            throw (new ErrnoException).useGlobalErrno("sigemptyset");
        }

        if (sigaction(SIGINT, &sa, &old_sigint) == -1)
        {
            throw (new ErrnoException).useGlobalErrno("sigaction (SIGINT)");
        }

        if (sigaction(SIGTERM, &sa, &old_sigterm) == -1)
        {
            throw (new ErrnoException).useGlobalErrno("sigaction (SIGTERM)");
        }

        if (sigaction(SIGRTMIN, &sa, &old_sigrtmin) == -1)
        {
            throw (new ErrnoException).useGlobalErrno("sigaction (SIGRTMIN)");
        }
    }

    /***************************************************************************

        Restores the original SIGINT handler.

    ***************************************************************************/

    private void restoreSignalHandlers ()
    {
        if (sigaction(SIGINT, &old_sigint, null) == -1)
        {
            throw (new ErrnoException).useGlobalErrno("sigaction (SIGINT)");
        }

        if (sigaction(SIGTERM, &old_sigterm, null) == -1)
        {
            throw (new ErrnoException).useGlobalErrno("sigaction (SIGTERM)");
        }

        if (sigaction(SIGRTMIN, &old_sigrtmin, null) == -1)
        {
            throw (new ErrnoException).useGlobalErrno("sigaction (SIGRTMIN)");
        }
    }

    /***************************************************************************

        Termination handler.

        Firstly unregisters all periodics.

        Secondly stops the node's select listener (stopping any more requests
        from being processed) and cancels any active requests.

        Thirdly calls the node's shutdown() method, shutting down the storage
        channels.

        Finally shuts down epoll. This will result in the run() method, above,
        returning.

        Returns:
            false in order not to register again with epoll.

    ***************************************************************************/

    private bool terminateApp ( )
    {
        try
        {
            Stdout.formatln("\nShutting down.");

            // Due to this delegate being called from epoll, we know that none of
            // the periodics are currently active.
            // Setting the terminating flag to true prevents any periodics which
            // fire from now on from doing anything (see IPeriodics).
            logger.info("Termination handler");

            // Stop checkpoint service
            logger.trace("Stopping checkpoint service.");
            this.checkpointer.stopService(this.epoll);
            logger.trace("Checkpoint service stopped.");

            // Wait for all workers to finish work, and destroy them
            this.async_io.destroy();

            logger.trace("Termination handler: shutting down periodics");
            this.timer_ext.clear();
            logger.trace("Termination handler: shutting down periodics finished");

            logger.trace("Termination handler: stopping node listener");
            this.node.stopListener(this.epoll);
            logger.trace("Termination handler: stopping node listener finished");

            logger.trace("Termination handler: shutting down node");
            this.node.shutdown();
            logger.trace("Termination handler: shutting down node finished");

            logger.trace("Termination handler: shutting down epoll");
            this.epoll.shutdown();
            logger.trace("Termination handler: shutting down epoll finished");

            logger.trace("Finished Termination handler");

            this.node.state = DlsNode.State.ShutDown;

            return false;
        }
        catch (Exception e)
        {
            logger.fatal("FAILED to terminate app: {}@{}:{}",
                    getMsg(e), e.file, e.line);
            throw e;
        }

        assert(false);
    }
}
