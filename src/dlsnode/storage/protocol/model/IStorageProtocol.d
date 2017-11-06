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

public import dlsnode.util.aio.JobNotification;

import dlsnode.storage.util.Promise;

interface IStorageProtocol
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

    public bool nextRecord (
            JobNotification suspended_job,
            BucketFile file, ref RecordHeader header );

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

    public Future!(RecordHeader) nextRecord (
            JobNotification job_notification,
            BucketFile file);

    /**************************************************************************

        Reads the next record value from the file.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header
            value = value buffer to read record value to

    **************************************************************************/

    public void readRecordValue (
            JobNotification suspended_job,
            BucketFile file, RecordHeader header, ref mstring value);

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

    public Future!(void[]) readRecordValue (
            JobNotification job_notification,
            BucketFile file, RecordHeader header);

    /**************************************************************************

        Skips the next record value in the file.

        Params:
            suspended_job = JobNotification to block
                the fiber on until read is completed
            file = bucket file instance to read from
            header = current record's header

    **************************************************************************/

    public void skipRecordValue (
            JobNotification suspended_job,
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
