/*******************************************************************************

    Links the elements into the list, making a queue. To be used with
    AioScheduler. `Queue.Element` should be inherited from the actual classes
    that will be queued by thread workers, and processed by the main thread.
    The reason for linking all these objects together and forming a queue in
    that way, as opposed to having dynamic array, or a wrapper struct is that
    in this way, worker threads never need to allocate memory for auxiliary
    structures, such as queue slots - they are always allocated by main thread,
    at the point of allocating the object that will actually be queued.  All
    this is done so that worker threads have zero interaction with druntime
    features, to be on the safe side of any inter-thread race conditions and
    race conditions with concurrent garbage collector.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.util.aio.internal.Queue;

/// ditto
static struct Queue
{
    /***************************************************************************

        Element of the queue. Classes that are to be queued should inherit
        from this class.

    ***************************************************************************/

    abstract static class Element
    {
        /***********************************************************************

            Link to the next element in the queue.

        ***********************************************************************/

        private Element next_node;
    }

    /***************************************************************************

        Head of the queue

    ***************************************************************************/

    private Element head;

    /***************************************************************************

        Tail of the queue

    ***************************************************************************/

    private Element tail;

    /***********************************************************************

        Inserts a suspendable request into the queue.

        Params:
            req = suspendable request to insert in the queue

    ***********************************************************************/

    public void insert ( Element req )
    {
        req.next_node = null;

        if (head is null)
        {
            this.head = req;
            this.tail = req;
        }
        else
        {
            this.tail.next_node = req;
            this.tail = req;
        }
    }

    /***********************************************************************

        Provides foreach iteration popping the suspendable requests from the
        queue.

        NB. This operation is destructive - iterating over elements
        will remove them.

        Params:
            dg = dg being called for every suspendable request in the queue

        Returns:
            return value of dg

    ***********************************************************************/

    public int opApply (int delegate (ref Element) dg)
    {
        int result;
        while (this.head)
        {
            auto req = this.head;
            this.head = cast(Element)this.head.next_node;
            assert(req !is null);

            result = dg(req);

            if (result)
            {
                break;
            }
        }

        return result;
    }

    /**********************************************************************

        Tells if the queue is empty.

        Returns:
            true if there are any suspendable requests in the queue,
            false otherwise.

    ***********************************************************************/

    public bool is_empty ()
    {
        return this.head is null;
    }
}
