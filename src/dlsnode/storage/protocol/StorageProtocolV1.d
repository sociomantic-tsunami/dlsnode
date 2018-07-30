/******************************************************************************

    Implementation of the V1 storage protocol, which
    includes parity checksum for record header

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.storage.protocol.StorageProtocolV1;

import ocean.transition;

import dlsnode.storage.protocol.model.IStorageProtocol;
import ocean.io.device.File;
import ocean.util.log.Logger;
import ocean.core.Verify;
import ocean.core.array.Mutation: copy;
import ocean.core.Exception;
import dlsnode.storage.util.Promise;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;

/*******************************************************************************

    Exception thrown on the bad parity.

*******************************************************************************/

public class ParityException: Exception
{
    mixin ReusableExceptionImplementation!();
}

/// ditto
private ParityException parity_exception;

static this ( )
{
    log = Log.lookup("dlsnode.storage.protocol.StorageProtocolV1");
    parity_exception = new ParityException;
}

/******************************************************************************

    Implements V1 storage layout. The records are stored one after another
    in the following fashion:

    ------------------------------------------------------------------------
    | key | len | chk |     value     | key | len | chk |  value | key | ...
    ------------------------------------------------------------------------
    \                /                 \               /
     \              /                   \             /
      \            /                     \           /
          header                             header

******************************************************************************/

scope class StorageProtocolV1: IStorageProtocol
{

    /**************************************************************************

        Reads next record header from the file, if any.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = record header to fill

        Returns:
            false if the record was read, true otherwise.

    **************************************************************************/

    public override bool nextRecord (
            JobNotification suspended_job,
            BucketFile file, ref RecordHeader header )
    {
        RecordHeaderV1 header_buf;

        if ( file.file_pos + header_buf.sizeof >= file.file_length )
        {
            return true; // end of bucket file
        }

        // Read header of next record
        file.readData(suspended_job,
                (cast(void*)&header_buf)[0..header_buf.sizeof]);

        // check the record header
        if (header_buf.calcParity() != 0)
        {
            // Record header is corrupted. Report that there are no more records
            log.warn("Record header parity check failed. Will not read any more "
                "records from this bucket file. File: {}, position: {}",
                file.file_path, file.file_pos);

            header = RecordHeader.init;
            return true;
        }

        // Checksum was correct
        header = header_buf.header;

        // Sanity check: if the length of the record is beyond the end of
        // the file, then just return. This can occur in two cases:
        //      1. A record being read as it is being written
        //      2. Corrupt data
        if ( file.file_pos + header.len > file.file_length )
        {
            return true; // end of bucket file
        }

        return false; // read header successfully
    }

    /**************************************************************************

        Tries to read the next record header from the file. In case the record
        can't be fetched, the request should suspend itself, wait to be resumed
        by job_notification and then collect results from the resulting future.

        Params:
            job_notification = JobNotification to wake up
                the fiber on the completion of the IO operation
            file = bucket file instance to read from

        Returns:
            future that either contains or will contain the next record's
            header.

    **************************************************************************/

    public override Future!(RecordHeader) nextRecord (
            JobNotification job_notification,
            BucketFile file)
    {
        auto future = file.readDataAsync!(RecordHeaderV1)(job_notification, RecordHeaderV1.sizeof);

        // Wrap the future into the conversion method to convert
        // from RecordHeaderV1 to RecordHeader
        return future.compose(&this.transformRecordHeader);
    }

    /***************************************************************************

        Converts RecordHeaderV1 to RecordHeader and checks the parity. Used
        as the future's transformation delegate.

        Params:
            buf = buffer containing RecordHeaderV1

        Returns:
            RecordHeader from the RecordHeaderV1.

        Throws:
            ParityException if the parity check fails.

    ***************************************************************************/

    private RecordHeader transformRecordHeader (in void[] buf)
    {
        RecordHeaderV1 read_header = *(cast(RecordHeaderV1*)(buf[0..RecordHeaderV1.sizeof]));

        auto parity = read_header.calcParity();
        // check the record header
        if (parity != 0)
        {
            // Record header is corrupted. Report that there are no more records
            log.warn("Record header parity check failed. Will not read any more "
                "records from this bucket file.");

            throw .parity_exception;
        }

        // Checksum was correct
        return read_header.header;
    }



    /**************************************************************************

        Reads the next record value from the file.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header
            value = value buffer to read record value to

    **************************************************************************/

    public override void readRecordValue (
            JobNotification suspended_job,
            BucketFile file, RecordHeader header, ref mstring value )
    {
        // Read value from file
        value.length = header.len;
        file.readData(suspended_job, value);
    }

    /**************************************************************************

        Tries to read the next record value from the file. In case the record
        can't be fetched, the request should suspend itself, wait to be resumed
        by job_notification and then collect results from the resulting future.

        Params:
            job_notification = JobNotification to wake up
                the fiber on the completion of the IO operation
            file = bucket file instance to read from
            header = current record's header

        Returns:
            future that either contains or will contain the record's value

    **************************************************************************/

    public override Future!(void[]) readRecordValue (
            JobNotification job_notification,
            BucketFile file, RecordHeader header)
    {
        return file.readDataAsync!(void[])(job_notification, header.len);
    }

    /**************************************************************************

        Skips the next record value in the file.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header

    **************************************************************************/

    public override void skipRecordValue (
            JobNotification suspended_job,
            BucketFile file, ref RecordHeader header )
    {
        file.seek(header.len, File.Anchor.Current);
    }

    /**************************************************************************

        Layouts the record to output buffer.

        Params:
            output = buffered output instance to write to
            key = record key
            value = record's value
            record_buffer = buffer used internally for rendering entire record before
                            passing it to BufferedOutput.

    **************************************************************************/

    public override void writeRecord (BufferedOutput output,
            hash_t key, cstring value, ref ubyte[] record_buffer)
    {
        RecordHeaderV1 header;

        header.header.key = key;
        header.header.len = value.length;

        // Calculate parity of the header
        header.parity = header.calcParity();

        // Preformat the data before sending it to BufferedOutput to
        // avoid flushing out of record boundary
        record_buffer.length =
            header.sizeof + (value.length * typeof (value[0]).sizeof);

        record_buffer[0 .. header.sizeof] = (cast(ubyte*)&header)[0 .. header.sizeof];
        record_buffer[header.sizeof .. $] = (cast(ubyte*)value.ptr)[0 .. value.length];

        output.append(record_buffer.ptr, record_buffer.length);
    }
}
