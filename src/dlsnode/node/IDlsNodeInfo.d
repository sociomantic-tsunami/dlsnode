/*******************************************************************************

    Information interface for DLS node

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.node.IDlsNodeInfo;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.node.model.IChannelsNodeInfo;



public interface IDlsNodeInfo : IChannelsNodeInfo
{
    /***************************************************************************

        DLS node state enum

    ***************************************************************************/

    public enum State
    {
        Running,
        Terminating,
        ShutDown
    }


    /***************************************************************************

        Returns:
            state of node

    ***************************************************************************/

    public State state ( );
}

