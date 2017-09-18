/*******************************************************************************

    FiberSelectEvent suspend/resume interface for suspendable contexts waiting
    for AsyncIO to finish.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.util.aio.EventFDContextAwaitingJob;

import ocean.io.select.client.FiberSelectEvent;
import ocean.io.select.fiber.SelectFiber;
import dlsnode.util.aio.ContextAwaitingJob;

/// ditto
class EventFDContextAwaitingJob: ContextAwaitingJob
{
    /***************************************************************************

        Constructor.

        Params:
            event = FiberSelectEvent to synchronise on.

    ***************************************************************************/

    this (FiberSelectEvent event)
    {
        this.event = event;
    }

    /***************************************************************************

        Constructor.

        Params:
            fiber = Fiber to create FiberSelectEvent to synchronise on.

    ***************************************************************************/

    this (SelectFiber fiber)
    {
        this.event = new FiberSelectEvent(fiber);
    }

    /***************************************************************************

        Yields the control to the suspendable context, indicating that the aio
        operation has been done.

    ***************************************************************************/

    protected override void wake_ ()
    {
        this.event.trigger;
    }

    /***************************************************************************

        Cedes the control from the suspendable context, waiting for the aio
        operation to be done.

    ***************************************************************************/

    protected override void wait_ ()
    {
        this.event.wait;
    }

    /***************************************************************************

        Changes the SelectFiber this handler is suspending

        Params:
            fiber = new SelectFiber to suspend

    ***************************************************************************/

    public void setFiber (SelectFiber fiber)
    {
        this.event.fiber = fiber;
    }

    /***************************************************************************

        FiberSelectEvent synchronise object.

    ***************************************************************************/

    private FiberSelectEvent event;
}
