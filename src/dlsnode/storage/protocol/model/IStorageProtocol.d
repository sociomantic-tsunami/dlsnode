/******************************************************************************

    Storage protocol with read and write from/to bucket files for every
    version of the bucket.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.storage.protocol.model.IStorageProtocol;

import ocean.transition;

public import dlsnode.storage.Record;
public import dlsnode.storage.BucketFile;
public import ocean.io.stream.Buffered;

public import dlsnode.util.aio.ContextAwaitingJob;

interface IStorageProtocol
{
    /**************************************************************************

        Reads next record header from the file, if any.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = record header to fill

        Returns:
            false if the record was read, true otherwise.

    **************************************************************************/

    public bool nextRecord (
            ContextAwaitingJob waiting_context,
            BucketFile file, ref RecordHeader header );


    /**************************************************************************

        Reads the next record value from the file.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header
            value = value buffer to read record value to

    **************************************************************************/

    public void readRecordValue (
            ContextAwaitingJob waiting_context,
            BucketFile file, RecordHeader header, ref mstring value);


    /**************************************************************************

        Skips the next record value in the file.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header

    **************************************************************************/

    public void skipRecordValue (
            ContextAwaitingJob waiting_context,
            BucketFile file, ref RecordHeader header );


    /**************************************************************************

        Layouts the record to output buffer.

        Params:
            output = buffered output instance to write to
            key = record key
            value = record's value
            record_buffer = buffer used internally for rendering entire record
                            passing it to BufferedOutput.

    **************************************************************************/

    public void writeRecord (BufferedOutput output, hash_t key, cstring value,
            ref ubyte[] record_buffer);

}
