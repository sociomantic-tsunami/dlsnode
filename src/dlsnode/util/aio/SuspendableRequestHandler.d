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

    protected abstract void wake_ ();

    /***************************************************************************

        Cedes the control from the suspendable request, waiting for the aio
        operation to be done. Implementation is defined by the concrete classes.

    ***************************************************************************/

    protected abstract void wait_();

    /***************************************************************************

        Cedes the control from the suspendable request, waiting for the aio
        operation to be done. Called by AsyncIO framework internally.

    ***************************************************************************/

    public final void wait ()
    {
        this.wait_();
    }

    /***************************************************************************

        Yields the control to the suspendable request, indicating that the aio
        operation has been done. Called by the AsyncIO framework internally.

    ***************************************************************************/

    public final void wake ()
    {
        this.wake_();
    }
}
