/*******************************************************************************

    Common interface for various types of suspendable requests waiting for
    AsyncIO to finish.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.util.aio.SuspendableRequestHandler;

/// Ditto
abstract class SuspendableRequestHandler
{
    /***************************************************************************

        Yields the control to the suspendable request, indicating that the aio
        operation has been done.

    ***************************************************************************/

    abstract public void wake ();

    /***************************************************************************

        Cedes the control from the suspendable request, waiting for the aio operation
        to be done.

    ***************************************************************************/

    abstract public void wait ();
}
