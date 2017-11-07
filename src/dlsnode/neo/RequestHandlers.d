/******************************************************************************

    Table of request handlers by command.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.neo.RequestHandlers;

import swarm.neo.node.ConnectionHandler;
import dlsproto.common.RequestCodes;

import GetRange = dlsnode.neo.request.GetRange;

/******************************************************************************

    This table of request handlers by command is used by the connection
    handler.  When creating a new request, the function corresponding to the
    request command is called in a fiber.

******************************************************************************/

public ConnectionHandler.RequestMap requests;

static this ( )
{
    requests.add(RequestCode.GetRange, "GetRange", &GetRange.handle);
}
