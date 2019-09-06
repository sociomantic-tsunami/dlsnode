/*******************************************************************************

    Promise/Future structure definitions.

    Promise/Future are used to transfer data during non-blocking async IO
    data read from the AsyncIO (the producer) to the consumers (such as
    request implementation). Promise and Future are two sides of the same coin -
    the producer owns the promise instances from which the future structure
    (containing _future_ result of the computation) is given to the consumer.

    The consumer can then use Future's methods to check if the data is available,
    to get the data or check if the error was raised.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.util.Promise;

import ocean.transition;
import ocean.core.Verify;
import ocean.core.Traits;
import core.sys.posix.sys.types;

/***************************************************************************

    "Promise" for the scheduled non-blocking async IO operation. Owned
    by the producer, wrapping the buffer, state and utility methods for
    transferring the data into the buffer and setting/resetting the state of
    the promise.

****************************************************************************/

public struct Promise
{
    /***********************************************************************

        Number of the bytes read so far during the IO.

    ***********************************************************************/

    private ssize_t bytes_last_read;


    /**********************************************************************

        Buffer with the data read.

    **********************************************************************/

    private void[] data_buffer;


    /**********************************************************************

        Current state of the promise.

    **********************************************************************/

    private enum State
    {
        /// Non-initialized
        Empty,
        /// Promise made
        PromiseMade,
        /// Promise has been fulfilled
        PromiseFulfilled,
        /// Promise has been fulfilled and the result was reaped
        PromiseReaped,
        /// Error has happened during processing
        Error
    }

    /// ditto
    private State state;


    /**********************************************************************

        Resets the promise to the initial state with indicator how many
        bytes should be read.

        Params:
            num_bytes = number of the bytes to read.

    **********************************************************************/

    public void reset (size_t num_bytes)
    {
        this.data_buffer.length = num_bytes;
        assumeSafeAppend(this.data_buffer);
        this.reset();
    }


    /**********************************************************************

        Initialises the promise to the initial state.

    **********************************************************************/

    public void reset ()
    {
        this.state = State.Empty;
        this.bytes_last_read = 0;
    }


    /***************************************************************************

        Sink method used by the producer to fill the result buffer.

        Params:
            result = result to copy into the data buffer.

    ***************************************************************************/

    public void fillResult (in void[] result)
    {
        if (result.length == 0) return;

        // Since the producer has setup this promise using reset(num_bytes)
        // this should never happen
        verify (this.bytes_last_read + result.length <= this.data_buffer.length);
        this.data_buffer[this.bytes_last_read..result.length] = result[];
        this.bytes_last_read += result.length;
    }


    /***************************************************************************

        Method used by the producer to mark the promise as fulfilled,
        with the error or without.

        Params:
            error = indicator if the error has happened.

    ***************************************************************************/

    public void fulfilled (bool error)
    {
        this.state = error? State.Error : State.PromiseFulfilled;
    }


    /***************************************************************************

        Returns:
            number of bytes missing for the promise to be fulfilled.

    ***************************************************************************/

    public size_t dataMissing ()
    {
        return this.data_buffer.length - this.bytes_last_read;
    }


    /**********************************************************************

        Returns:
            future associated with this promise.

    **********************************************************************/

    public Future!(T) getFuture(T)()
    {
        return Future!(T)(&this);
    }


    /**********************************************************************

        Gets the result from the promise.

        Returns:
            result of the finished async IO operation

    **********************************************************************/

    private void[] result ()
    {
        verify(this.promise_fulfilled());
        this.state = State.PromiseReaped;
        return this.data_buffer[0..this.bytes_last_read];
    }


    /**********************************************************************

        Checks if the producer has finished processing and it's safe
        to collect the results.

    **********************************************************************/

    private bool promise_fulfilled ()
    {
        return this.state == State.PromiseFulfilled || this.state == State.Error;
    }


    /**********************************************************************

        Checks if the promise has been reaped.

    ***********************************************************************/

    private bool promise_reaped ()
    {
        return this.state == State.PromiseReaped;
    }


    /**********************************************************************

        Check if there was any error during processing.

        Returns:
            true if there was an error during the processing

    ***********************************************************************/

    private bool error ()
    {
        verify(this.promise_fulfilled());
        return this.state == State.Error;
    }
}

/**************************************************************************

    Struct encapsulating "future" result that promise will provide. It's the
    other side of the promise/future coin - promise is in the ownership of
    the producer and future is in the ownership of the consumer.

    Params:
        T = type of the result to wrap the future around

**************************************************************************/

struct Future(T = void[])
{
    /// Promise that owns this future. Can be null in case of empty
    /// future placeholder.
    private Promise* promise;

    /// Conversion method to convert the promise's data into the future
    private T delegate(in void[]) convert_method;

    /**********************************************************************

        Returns:
            indicator if the future is ready for collecting the result.

    ***********************************************************************/

    public bool valid ()
    {
        return
            promise !is null &&
            promise.promise_fulfilled() &&
            !promise.promise_reaped();
    }

    /**********************************************************************

        Collects the result from this future. Should be called only if
        this.valid is true. this.valid() will return false after this call
        has been made.

        Returns:
            the result stored in this future.

    **********************************************************************/

    public T get ()
    {
        verify(this.valid());

        if (convert_method !is null)
        {
            return this.convert_method(this.promise.result());
        }
        else
        {
            static if (isDynamicArrayType!(T))
            {
                return cast(T)this.promise.result();
            }
            else
            {
                auto result = this.promise.result();
                return *(cast(T*)result[0..T.sizeof].ptr);
            }
        }
    }

    /**********************************************************************

        Indicates if there was an error while fulfilling the promise.

        Returns:
            true if the error has happened during fulfilling the promise.

    ***********************************************************************/

    public bool error ()
    {
        return this.promise.error();
    }

    /**************************************************************************

        Wraps the original future into a new future, using the conversion
        function to perform conversion from the result of the original future
        into the new future.

        // TODO: this allows only for a single conversion step
        // maybe we can do better and allow unlimited depth

    **************************************************************************/

    public Future!(U) compose(U)(scope U delegate(in void[]) convert)
    {
        Future!(U) future;
        future.promise = this.promise;
        future.convert_method = convert;
        return future;
    }
}
