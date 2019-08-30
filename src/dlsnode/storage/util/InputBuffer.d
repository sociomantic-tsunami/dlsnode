/******************************************************************************

    This module provides InputBuffer class which provides regular File's
    API, but uses buffering to reduce the number of actual system calls,
    and instead works on in-memory data.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.util.InputBuffer;

import ocean.transition;

import core.sys.posix.sys.types;
import ocean.core.Buffer;
import ocean.core.Verify;
import ocean.io.device.File;
import core.stdc.string;
import ocean.core.array.Mutation;

import dlsnode.storage.util.Promise;

version (UnitTest)
{
    import ocean.core.Test;
}

/******************************************************************************

    InputBuffer implementation

*******************************************************************************/

struct InputBuffer
{
    /**************************************************************************

        Slice to user-provided buffer to store the read data into.

    **************************************************************************/

    private void[] buffer;

    /**************************************************************************

        Amount of data currently being in chunk.

    **************************************************************************/

    private size_t data_in_chunk;

    /**************************************************************************

        Current cursor position inside a chunk.

    **************************************************************************/

    private size_t position_in_chunk;


    /***************************************************************************

        Indicator that there's no more data available in the input.

    ***************************************************************************/

    private bool feof_occured;


    /**************************************************************************

        Promise to fill after reading data into buffer.

    **************************************************************************/

    private Promise* promise;


    /**************************************************************************

        Consumes dest.length bytes from the internal buffer and fills the
        `dest` keeping the track of the position in the internal buffer.

        Params:
            dest = destination buffer to fill from the internal buffer.

    **************************************************************************/

    private void consumeFromInternalBuffer (void[] dest)
    in
    {
        assert ((&this).buffer.length - (&this).position_in_chunk >= dest.length);
    }
    body
    {
        dest[0 .. $] =
            (&this).buffer[(&this).position_in_chunk .. (&this).position_in_chunk + dest.length];
        (&this).position_in_chunk += dest.length;
    }

    /**************************************************************************

        Consumes required.length bytes from the internal buffer and sinks them
        into the `sink` delegate keeping the track of the position in the
        internal buffer.

        Params:
            required = number of bytes required to consume
            sink = sink delegate to fill from the internal buffer.

    **************************************************************************/

    private void consumeFromInternalBuffer (size_t required, scope void delegate(in void[] dest) sink)
    {
        verify((&this).buffer.length - (&this).position_in_chunk >= required);

        sink(
            (&this).buffer[(&this).position_in_chunk .. (&this).position_in_chunk + required]);
        (&this).position_in_chunk += required;
    }

    /*************************************************************************

        Resets the object to the clean state, specifying the input buffer.
        The buffer can be null, in which case the class will perform
        no buffering at all.

        Params:
            buffer = buffer to use to store the buffered data.

    *************************************************************************/

    public void reset (void[] buffer)
    {
        (&this).reset();
        (&this).buffer = buffer;
    }

    /*************************************************************************

        Resets the object to the clean state, still using the same buffer (if any).

    *************************************************************************/

    public void reset ()
    {
        (&this).position_in_chunk = 0;
        (&this).feof_occured = false;
        (&this).data_in_chunk = 0;
    }

    /**************************************************************************

        Seeks inside the file.

        This method seeks inside the file, or just inside the buffer if the
        seek amount is low enough to stay within the bounds.

        Params:
            offset = number of bytes to seek
            anchor = file anchor to seek from
            seek_input = delegate used to perform the seek inside the input

        Returns:
            position of cursor after seek


    **************************************************************************/

    public ssize_t seek (long offset, File.Anchor anchor,
            scope ssize_t delegate (long, File.Anchor) seek_input)
    {
        // Check if we can just seek inside the provided buffer
        if ((&this).buffer.length && anchor == File.Anchor.Current &&
                (offset + (&this).position_in_chunk < (&this).data_in_chunk))
        {
            (&this).position_in_chunk += offset;
            return seek_input(0, File.Anchor.Current) + (&this).position_in_chunk;
        }
        else
        {
            auto new_position_in_input = seek_input(offset, anchor);
            (&this).reset((&this).buffer);
            return new_position_in_input;
        }
    }

    /**************************************************************************

        Returns the remaining nubmer of bytes in buffer.

        Returns:
            remaining number of bytes in buffer.

    **************************************************************************/

    public size_t remainingInBuffer ( )
    {
        return (&this).data_in_chunk - (&this).position_in_chunk;
    }

    /***************************************************************************

        Tries to read data in non-blocking manner. If the requested data is
        available in the internal buffer, this method would return immediately,
        filling the buffer with the requested data. Otherwise, provided method
        async_read_data will be called, expecting to fill the internal buffer 
        and call asyncReadCompleted to let InputBuffer update it's state with the data
        actually read from the file. Either way, this method will return
        *immediately* and user should check if the read has completed inspecting
        the Future object.

        Params:
            promise = promise to fulfil
            async_read_data = delegate to read the data and let the input buffer know
                        when it's done (via asyncReadCompleted parameter).

    ***************************************************************************/

    public void asyncReadData (ref Promise promise,
            scope void delegate(void[] dest,
                          void delegate(ssize_t) asyncReadCompleted) async_read_data)
    {
        // If we don't need to read anything
        if (promise.dataMissing() == 0)
        {
            promise.fulfilled(false);
            return;
        }

        // Do we have enough data already in the buffer?
        if ((&this).remainingInBuffer() >= promise.dataMissing)
        {
            (&this).consumeFromInternalBuffer(promise.dataMissing(), &promise.fillResult);
            promise.fulfilled(false);
            return;
        }
        else if ((&this).feof_occured)
        {
            // Copy what we can and return that.
            auto remaining = (&this).remainingInBuffer();
            (&this).consumeFromInternalBuffer(remaining, &promise.fillResult);
            promise.fulfilled(true);
            return;
        }

        // There's not enough data in the buffer? Let's move the remaining of the
        // data to the front of the buffer, and refill the buffer with the new data
        auto remaining = (&this).moveRemainingToFront();

        // If user is asking for more data than the size of the internal buffer
        // we'll resize the buffer first
        if ((&this).buffer.length < promise.dataMissing)
        {
            (&this).buffer.length = promise.dataMissing;
            enableStomping((&this).buffer);
        }

        // Set the final destination buffer to read from
        (&this).promise = &promise;
        async_read_data((&this).buffer[remaining..$], &(&this).asyncReadCompleted);
    }

    /**************************************************************************

        Reads data from the file to fill the provided destination array.
        Data is first read from the buffered data in memory, then from the
        actual file on disk, if the buffered data is exhausted

        Params:
            dest = buffer to fill with data from the file
            read_data = delegate to call in order to bring the data into the
                buffer

        Returns:
            number of bytes read from the file

    **************************************************************************/

    public size_t readData (void[] dest,
            scope ssize_t delegate (void[] dest) read_data)
    {
        if (dest.length == 0)
        {
            return 0;
        }

        // If the buffer doesn't exist anyway, we don't need it - just forward
        // the stuff from file
        if ((&this).buffer.length == 0)
        {
            // Non buffered read - read directly from the input
            return read_data(dest);
        }

        // Do we have enough data already in the buffer?
        if (((&this).data_in_chunk - (&this).position_in_chunk) >= dest.length)
        {
            (&this).consumeFromInternalBuffer(dest[0 .. $]);
            return dest.length;
        }

        // At this point we know we don't have enough data in the buffer to
        // fulfil the request. So, we'll copy to the destination buffer what we have,
        // and will read data from the input, filling the destination buffer.

        // Copy the remaining data from the buffer to the output
        auto remaining_in_internal_buffer = (&this).data_in_chunk - (&this).position_in_chunk;
        (&this).consumeFromInternalBuffer(dest[0 .. remaining_in_internal_buffer]);

        auto bytes_needed_for_output_buffer = dest.length - remaining_in_internal_buffer;
        auto already_copied_to_output = remaining_in_internal_buffer;

        // Read the rest from the input source. We have two possibilities here:
        //
        // 1. The amount of data requested is larger than the internal buffer
        // size - read the data directly and fill the output buffer.
        //
        // 2. The amount of data requested is smaller than the internal buffer
        // size - read the entire internal buffer's size worth of data, fill
        // the output buffer, and leave the remaining in the internal buffer

        if (bytes_needed_for_output_buffer >= (&this).buffer.length)
        {
            auto bytes_read = read_data(dest[already_copied_to_output .. $]);

            // We're done, nothing more to read from the file
            return already_copied_to_output + bytes_read;
        }
        else
        {
            // Read the data to the internal buffer
            auto bytes_read = read_data((&this).buffer[0 .. (&this).buffer.length]);
            (&this).data_in_chunk = bytes_read;
            (&this).position_in_chunk = 0;

            // We have read `bytes_read`. Fill the output buffer with the
            // data in the internal buffer.
            auto to_copy_from_internal = bytes_needed_for_output_buffer > bytes_read ?
                bytes_read : bytes_needed_for_output_buffer;

            // Consume the number of needed bytes from the internal buffer
            (&this).consumeFromInternalBuffer(
                    dest[already_copied_to_output .. already_copied_to_output + to_copy_from_internal]);

            return already_copied_to_output + to_copy_from_internal;
        }
    }

    /***************************************************************************

        Reorders buffer in the way that all unused elements are moved to the
        front of the buffer, making the end of the buffer available for a refill.

        Returns:
            amount of remaining elements in the buffer

    ***************************************************************************/

    private size_t moveRemainingToFront ()
    {
        auto remaining = (&this).remainingInBuffer();

        void* src = buffer.ptr + (&this).position_in_chunk;
        void* dst = buffer.ptr;
        memmove(dst, src, remaining);

        (&this).position_in_chunk = 0;
        (&this).data_in_chunk = remaining;
        return remaining;
    }

    /***************************************************************************

        Should be called from AIO subsystem when the requested AIO read has
        been completed. It updates buffer's internal state to reflect the
        result of the nonblocking read.

        Params:
            bytes_read = number of bytes read

    ***************************************************************************/

    private void asyncReadCompleted (ssize_t bytes_read)
    {
        if (bytes_read < 0)
        {
            (&this).promise.fulfilled(true);
            return;
        }

        if (bytes_read == 0)
        {
            (&this).feof_occured = true;
        }

        (&this).data_in_chunk += bytes_read;

        // Now let's fill the consumer's buffer
        // Do we have enough data already in the buffer?
        if ((&this).remainingInBuffer() >= (&this).promise.dataMissing)
        {
            (&this).consumeFromInternalBuffer((&this).promise.dataMissing,
                &(&this).promise.fillResult);
            (&this).promise.fulfilled(false);
        }
        else if ((&this).feof_occured)
        {
            // Copy what we can and return that.
            auto remaining = (&this).remainingInBuffer();
            (&this).consumeFromInternalBuffer(remaining, &(&this).promise.fillResult);
            (&this).promise.fulfilled(true);
        }
    }
}

unittest
{
    // Fill the input with the values from [0, 1000)
    ubyte[] input;
    input.length = 1000;

    for (auto i = 0; i < input.length; i++)
    {
        input[i] = cast(ubyte)i;
    }

    // Position in the buffer (i.e. file cursor)
    size_t pos = 0;

    // read data method
    ssize_t read_data (void[] buf)
    {
        ssize_t read = 0;
        for (auto i = 0; i < buf.length; i++)
        {
            if (pos == input.length)
            {
                return read;
            }

            (cast(ubyte[])buf)[i] = input[pos++];
            read++;
        }

        return read;
    }

    // seek method
    ssize_t seek (long seek_pos, File.Anchor anchor)
    {
        auto new_pos = pos;
        switch (anchor)
        {
            case File.Anchor.Begin:
                new_pos = seek_pos;
                break;

            case File.Anchor.End:
                new_pos = input.length + seek_pos;
                break;

            case File.Anchor.Current:
                new_pos += seek_pos;
                break;

            default:
                assert(false);
        }

        if (new_pos > input.length)
        {
            new_pos = input.length;
        }

        if (new_pos < 0)
        {
            return -1;
        }

        pos = new_pos;

        return pos;
    }

    InputBuffer buffer;

    ubyte[] dest;
    Buffer!(void) buffer_array;

    // Test 0 - ask for 0 bytes
    test!("==")(buffer.readData(dest, &read_data), 0);

    // Test 1 - using 0 length buffer and reading everything
    buffer.reset(buffer_array[]);

    dest.length = input.length;

    buffer.readData(dest, &read_data);

    for (auto i = 0; i < input.length; i++)
    {
        test!("==")(input[i], dest[i]);
    }

    // Test 2 - using 1byte BufferedInput
    pos = 0;
    buffer_array.length = 1;
    buffer.reset(buffer_array[]);

    dest.length = input.length;

    buffer.readData(dest, &read_data);
    for (auto i = 0; i < input.length; i++)
    {
        test!("==")(input[i], dest[i]);
    }

    // Test 3 - using 125 bytes buffer
    pos = 0;
    buffer_array.length = 125;
    buffer.reset(buffer_array[]);

    dest.length = input.length;

    buffer.readData(dest, &read_data);
    for (auto i = 0; i < input.length; i++)
    {
        test!("==")(input[i], dest[i]);
    }

    // Test 4 & 5 - try with seek

    int try_number = 0;
    ubyte[] tmp;
    int already_compared = 0;

    while (try_number++ < 3)
    {
        // Test 4 - read in small chunks
        pos = 0;
        buffer.reset(buffer_array[]);
        tmp.length = 3;
        already_compared = 0;

        // read first 9 times, for the first
        // 105 * 9 = 945 bytes
        for (int i = 0; already_compared + tmp.length < input.length; i++)
        {
            auto read = buffer.readData(tmp, &read_data);
            test!("==")(read, tmp.length);

            for (auto j = 0; j < tmp.length; j++)
            {
                test!("==")(input[j + already_compared], tmp[j]);
            }

            already_compared += tmp.length;
        }

        // read the remaining
        auto expected_number = input.length - already_compared;
        test!("==")(buffer.readData(tmp, &read_data), expected_number);

        for (auto i = 0; i < expected_number; i++)
        {
            test!("==")(input[i + already_compared], tmp[i]);
        }

        buffer.seek(0, File.Anchor.Begin, &seek);
    }

    // Test 5 - following scenario:
    // Use 125 bytes buffer and read 50 bytes, and then
    // 25 bytes, which should be inside buffer, and then
    // read remaining bytes from the input in one go, which should
    // not use the buffering at all.
    pos = 0;
    buffer.reset(buffer_array[]);

    already_compared = 0;
    tmp.length = 50;

    test!("==")(buffer.readData(tmp, &read_data), tmp.length);

    for (auto j = 0; j < tmp.length; j++)
    {
        test!("==")(input[j + already_compared], tmp[j]);
    }
    already_compared += tmp.length;

    // Read next 25 bytes
    tmp.length = 25;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);

    for (auto j = 0; j < tmp.length; j++)
    {
        test!("==")(input[j + already_compared], tmp[j]);
    }
    already_compared += tmp.length;

    // And try to read the next input.length bytes
    tmp.length = input.length;
    auto bytes_read = buffer.readData(tmp, &read_data);

    test!("==")(bytes_read, input.length - 50 - 25);
    for (auto j = 0; j < bytes_read; j++)
    {
        test!("==")(input[j + already_compared], tmp[j]);
    }

    // Test 6 - read the chunk of the data, buffering something, and then read
    // more than a buffer, discarting stuff in the bffer

    // Test 7 - read the chunk of the data from the already open file
    auto start_pos = input.length / 2;
    pos = start_pos;
    buffer.reset(buffer_array[]);

    // Try to read everything, but we should really get only half of it
    tmp.length = input.length;
    test!("==")(buffer.readData(tmp, &read_data), input.length / 2);


    // Test 8 - read entire input from a smaller buffer
    pos = 0;
    buffer.reset(buffer_array[]);
    tmp.length = input.length;
    test!("==")(buffer.readData(tmp, &read_data), input.length);

    // confirm contents
    for (auto i = 0; i < input.length; i++)
    {
        test!("==")(input[i], tmp[i]);
    }

    // Test 9 - given the buffer of 125 bytes - read 100 bytes,
    // and then another 100 bytes - which should consume from buffer,
    // then read the remaining stuff

    pos = 0;
    buffer.reset(buffer_array[]);
    tmp.length = 100;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);

    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[i], tmp[i]);
    }

    // read next 100
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);
    
    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[100 + i], tmp[i]);
    }

    // Test 10 - given the buffer of 125 bytes - read 100 bytes, and
    // then read 150 bytes
    pos = 0;
    buffer.reset(buffer_array[]);
    tmp.length = 100;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);

    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[i], tmp[i]);
    }

    // read next 150
    tmp.length = 150;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);
    
    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[100 + i], tmp[i]);
    }

    // test 11 - given the buffer of 125 bytes, read 100, then read 1,
    // then read 100
    pos = 0;
    buffer.reset(buffer_array[]);
    tmp.length = 100;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);

    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[i], tmp[i]);
    }

    // read next 1
    tmp.length = 1;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);
    
    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[100 + i], tmp[i]);
    }

    // read next 100
    tmp.length = 100;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);
    
    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[100 + 1 + i], tmp[i]);
    }

    // read next 100
    tmp.length = 100;
    test!("==")(buffer.readData(tmp, &read_data), tmp.length);
    
    // confirm contents
    for (auto i = 0; i < tmp.length; i++)
    {
        test!("==")(input[100 + 100 + 1 + i], tmp[i]);
    }
}
