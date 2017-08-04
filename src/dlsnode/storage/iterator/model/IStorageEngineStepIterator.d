/*******************************************************************************

    Abstract base class for DLS node step-by-step iterators. An iterator
    class must be implemented for each storage engine.

    A step iterator is distinguished from an opApply style iterator in that it
    has explicit methods to get the current key / value, and to advance the
    iterator to the next key. This type of iterator is essential for an
    asynchronous storage engine, as multiple iterations could be occurring in
    parallel (asynchronously), and each one needs to be able to remember its own
    state (ie which record it's up to, and which is next). This class provides
    the interface for that kind of iterator.

    This abstract iterator class has no methods to begin an iteration. As
    various different types of iteration are possible, it is left to derived
    classes to implement suitable methods to start iterations.

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.iterator.model.IStorageEngineStepIterator;
import dlsnode.util.aio.SuspendableRequestHandler;

import ocean.transition;

interface IStorageEngineStepIterator
{
    /***************************************************************************

        Initialises the iterator to iterate over all records in the
        storage engine. The first key is queued up, ready to be fetched
        with the methods below.

        Params:
            suspendable_request_handler = SuspendableRequestHandler instance to block the caller on.

    ***************************************************************************/

    public void getAll ( SuspendableRequestHandler suspendable_request_handler );


    /***************************************************************************

        Initialises the iterator to iterate over all records in the
        storage engine within the specified range of keys. The first key
        in the specified range is queued up, ready to be fetched with
        the methods below.

        Params:
            suspendable_request_handler = SuspendableRequestHandler instance to block the caller on.
            min = string containing the hexadecimal key of the first
                record to iterate
            max = string containing the hexadecimal key of the last
                record to iterate

    ***************************************************************************/

    public void getRange ( SuspendableRequestHandler suspendable_request_handler, cstring min, cstring max );


    /***************************************************************************

        Gets the key of the current record the iterator is pointing to.

        Returns:
            current key

    ***************************************************************************/

    public cstring key ( );


    /***************************************************************************

        Gets the value of the current record the iterator is pointing
        to.

        Params:
            event = SuspendableRequestHandler to block the fiber on until read is completed

        Returns:
            current value

    ***************************************************************************/

    public cstring value ( SuspendableRequestHandler suspendable_request_handler );


    /***************************************************************************

        Advances the iterator to the next record or to the first record in
        the storage engine, if this.started is false.

        Params:
            event = SuspendableRequestHandler to block the fiber on until read is completed

    ***************************************************************************/

    public void next ( SuspendableRequestHandler suspendable_request_handler );


    /***************************************************************************

        Tells whether the current record pointed to by the iterator is the last
        in the iteration.

        This method may be overridden, but the default definition of the
        iteration end is that the current key is empty.

        Returns:
            true if the current record is the last in the iteration

    ***************************************************************************/

    public bool lastKey ( );


    /***************************************************************************

        Aborts the current iteration. Causes lastKey() (the usual condition
        which is checked to indicate the end of the iteration) to always return
        true, until the iteration is restarted.

    ***************************************************************************/

    public void abort ( );
}
