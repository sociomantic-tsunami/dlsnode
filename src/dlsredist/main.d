/******************************************************************************

    Dls redistribution utility. Initiates `Redistribute` request to the source
    nodes, passing them list of the new nodes and the fraction of data to
    distribute.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsredist.main;


/******************************************************************************

    Imports.

******************************************************************************/

import Version;

import ocean.transition;

import dlsredist.client.DlsClient;

import ocean.core.Enforce: enforce;
import ocean.io.Stdout;
import ocean.io.select.EpollSelectDispatcher;
import ocean.util.app.CliApp;

import swarm.Const;
import swarm.client.model.IClient;
import swarm.client.model.ClientSettings;
import swarm.client.helper.NodesConfigReader;
import dlsproto.client.legacy.internal.request.params.RedistributeInfo;

import ocean.util.Convert;


/******************************************************************************

    Main function.

    Params:
        args = array with raw command line arguments

******************************************************************************/

version (UnitTest) {} else
int main ( istring[] args )
{
    auto app = new DlsRedist;
    return app.main(args);
}


/******************************************************************************

    Main application class.

******************************************************************************/

public class DlsRedist: CliApp
{
    import ocean.text.Arguments : Arguments;

    /**************************************************************************

        EpollSelectDispatcher instance.

    **************************************************************************/

    private EpollSelectDispatcher epoll;


    /**************************************************************************

        Constructor.

    **************************************************************************/

    public this ( )
    {
        static immutable name = "dlsredist";
        static immutable desc = "initiates a redistribution of DLS data";
        static immutable help =
`Tool to initialte a redistribution of data within a
DLS. The standard use case when adding a new nodes to DLS is as follows:
    1. Setup a new nodes as required.
    2. Prepare a list of all new nodes.
    3. Pass the list of all old nodes via src and a list of all new nodes via
       dst parameters.`;

        OptionalSettings settings;
        settings.help = help;

        super (name, desc, version_info, settings);
        this.epoll = new EpollSelectDispatcher;
    }

    /***************************************************************************

        Sets up the command line arguments which the application can handle.

        Params:
            app = application instance
            args = args set up instance

     ***************************************************************************/

    public override void setupArgs ( IApplication app, Arguments args )
    {
        args("src").aliased('S').required().params(1).
            help("File describing DLS -- should contain the address/port of " ~
                 "all source nodes");

        args("dst").aliased('D').required().params(1).
            help("File describing destination nodes -- should contain the " ~
                 "address/port of all destination nodes");

        args("fraction").required().aliased('f').params(1).
            help("Fraction of data to send from old to new nodes.");
    }

    /**************************************************************************

        Checks whether the command line arguments are valid.

        Params:
           app = application instance
           args = args set up instance

        Returns:
           error message or null if arguments are ok

    ***************************************************************************/

    protected override cstring validateArgs ( IApplication app, Arguments args )
    {
        if ( !args("src").assigned || !args("dst").assigned)
        {
            return "Source and destination list of nodes must be non-empty";
        }

        if ( !args("fraction").assigned )
        {
            return "Fraction of data to redistribute must be in range (0, 1]";
        }

        float f = to!(float)(args.getString("fraction"));

        if (!(f > 0 && f <= 1))
        {
            return "Fraction of data to redistribute must be in (0, 1] range";
        }

        return null;
    }

    /***************************************************************************

        Application main run method. Parses arguments and runs the application.

        Creates a client, handshakes with the source nodes,
        loads the fraction of data to send, makes a list
        of the destination nodes and fires off `Redistribute` requests.

        Params:
            args = command line arguments as an Arguments instance

        Returns:
            status code to return to the OS

    **************************************************************************/

    protected override int run ( Arguments args )
    {
        float fraction_of_data_to_send = to!(float)(args.getString("fraction"));

        auto dls = new DlsClient(this.epoll);
        auto nodes = args.getString("src");
        dls.addNodes(nodes);
        this.handshake(dls);

        NodeItem[] dst;

        foreach (node; NodesConfigReader(args.getString("dst")))
        {
            dst ~= node;
        }

        this.redistribute(dls, dst, fraction_of_data_to_send);

        return 0;
    }

    /**************************************************************************

        Performs a handshake with the source nodes.

        Params:
            dls = DlsClient to initiate handshake for.

    **************************************************************************/

    private void handshake ( DlsClient dls )
    {
        bool error;

        void handshake ( DlsClient.RequestContext, bool ok )
        {
            if (!ok)
            {
                error = true;
            }
        }

        void notifier (DlsClient.RequestNotification info )
        {
            mstring msg_buf;
            if (info.type == info.type.Finished && !info.succeeded )
            {
                Stderr.formatln("Handshake failed: {}",
                        info.message(msg_buf));
                error = true;
            }
        }

        dls.nodeHandshake(&handshake, &notifier);

        this.epoll.eventLoop();

        enforce(!error, "DLS handshake failed");
    }

    /**************************************************************************

        Sends of Redistribute request to all source nodes, sending them list
        of the destination nodes and a fraction of the data to send.

        Params:
            dls = DlsClient instance
            dst = list of the destination nodes
            fraction_of_data_to_send = fraction of data to pass from src -> dst

    **************************************************************************/

    private void redistribute (DlsClient dls, NodeItem[] dst,
            float fraction_of_data_to_send)
    {
        RedistributeInfo get_nodes (DlsClient.RequestContext context)
        {
            RedistributeInfo info;
            info.redist_nodes = dst;
            info.fraction_of_data_to_send = fraction_of_data_to_send;
            return info;
        }

        void notifier ( DlsClient.RequestNotification info )
        {
            if (info.type == info.type.Finished && !info.succeeded)
            {
                Stderr.red.formatln("Error performing redistribute: {}",
                        info.message(dls.msg_buf)).default_colour.flush;
            }
        }

        dls.assign(dls.redistribute(&get_nodes, &notifier));

        this.epoll.eventLoop();
    }
}
