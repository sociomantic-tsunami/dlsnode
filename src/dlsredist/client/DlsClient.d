/******************************************************************************

    Custom DlsClient with the support for Redistribute request.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsredist.client.DlsClient;


/******************************************************************************

    Imports

******************************************************************************/

import SwarmClient = dlsproto.client.DlsClient;

/******************************************************************************

    Custom DlsClient with support for Redistribute request.

******************************************************************************/

class DlsClient: SwarmClient.DlsClient
{
    import swarm.util.ExtensibleClass;
    import ocean.core.Verify;

    import dlsproto.client.legacy.DlsConst;
    import dlsproto.client.legacy.internal.RequestSetup;

    import swarm.Const: NodeItem;
    import dlsproto.client.legacy.internal.request.params.RedistributeInfo;


    /***************************************************************************

        Constructor.

        Params:
            epoll = EpollSelectDispatcher instance.

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll )
    {

        super (epoll);
    }


    /**************************************************************************

        Redistribute Request

    **************************************************************************/

    private struct Redistribute
    {
        mixin RequestBase;
        mixin IODelegate;

        mixin RequestParamsSetup;
    }

    public Redistribute redistribute (  RequestParams.RedistributeDg input,
            RequestNotification.Callback notifier )
    {
        return *Redistribute(DlsConst.Command.E.Redistribute, notifier).io(input);
    }
}
