/******************************************************************************

    Implementation of the Redistribute request.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.request.RedistributeRequest;


/******************************************************************************

    Imports

******************************************************************************/

import Protocol = dlsproto.node.request.Redistribute;

import ocean.transition;

import ocean.util.log.Logger;

/******************************************************************************

    Static module logger.

******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.request.RedistributeRequest");
}


/******************************************************************************

    Request Implementation.

******************************************************************************/

scope class RedistributeRequest: Protocol.Redistribute
{
    import dlsnode.request.model.ConstructorMixin;
    import dlsnode.storage.StorageEngine;
    import dlsnode.storage.iterator.StorageEngineFileIterator;
    import dlsnode.storage.BucketFile;
    import dlsnode.storage.Record;

    import swarm.Const: NodeItem;
    import Hash = swarm.util.Hash;
    import dlsproto.client.legacy.DlsConst;
    import dlsnode.connection.client.DlsClient;

    import ocean.core.Array: copy;
    import ocean.core.Buffer;
    import ocean.core.TypeConvert : downcast;

    import ocean.text.convert.Formatter;
    import ocean.io.FilePath;
    import ocean.io.device.File;
    import ocean.math.random.Random;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();


    /***************************************************************************

        Bytes transfered during the redistribution

    ***************************************************************************/

    private ulong bytes_redistributed;


    /***************************************************************************

        Records transfered during the redistribution

    ***************************************************************************/

    private ulong records_redistributed;


    /***************************************************************************

        Buffer used for buffered input while reading the source buckets.

        NOTE: this field is static, as this class is instantiated as scope
        class, so having it just as a field will lead to a memory leak. Having
        it as a resource is also not ideal, as all the buffers in the pool will
        tend to grow to reach the average maximum. Since RedistributeRequest is
        enforced to have only one active request at the time, this field is set
        to static.

    ***************************************************************************/

    private static Buffer!(void) input_buffer;

    /***************************************************************************

        Performs the redistibution of the data to other nodes specified in the
        dst_nodes.

        Params:
            dst_nodes = list of the destination nodes to redistribute data to.
            fraction_of_data_to_send = fraction of the data to send away to
                                       destination nodes.

    ***************************************************************************/

    final override protected void redistributeData ( NodeItem[] dst_nodes,
           float fraction_of_data_to_send )
    {
        // We don't want events on the connection handler socket (to the dls
        // client) to mess with the fiber any more, as it's about to start
        // being managed by the ScopeRequests instance of the internal dls
        // client (see the call to DlsClient.perform() in forwardBucket() below).
        // Simply unregistering the socket from epoll prevents any unexpected
        // events from interrupting the flow of the request.
        this.reader.fiber.unregister();

        .log.info("Starting redistribution.");

        // reset the stats
        this.bytes_redistributed = 0;
        this.records_redistributed = 0;

        // setup dls client
        auto client = this.resources.dls_client();
        client.clearNodes();

        // add all destination nodes
        foreach (node; dst_nodes)
        {
            client.addNode(node.Address, node.Port);
        }

        // iterate over channels, redistributing the data.
        foreach ( channel; this.resources.storage_channels )
        {
            auto dls_channel = downcast!(StorageEngine)(channel);
            assert(dls_channel);

            try
            {
                this.handleChannel(client, dls_channel,
                        fraction_of_data_to_send);
            }
            catch ( Exception e )
            {
                .log.error("Exception thrown while redistributing channel '{}': "
                        "'{}' @ {}:{}", channel.id, e.msg, e.file, e.line);
            }
        }

        .log.info("Redistribution has been completed. "
                  "Transfered {} records with {} bytes",
                  this.records_redistributed, this.bytes_redistributed);
    }


    /**************************************************************************

        Iterates over the given storage engine, forwarding the amount of
        records specified by requests `fraction_data_to_send`. If an error
        occurs, while forwarding one of more records, those records are simply
        reforwarded to the next node.
        As `PutBatch` is a single-node request, that will be used to
        distribute buckets to all destination nodes in the round-robin fashion.

        Params:
            client = dls client instance
            channel = channel to redistribute
            fraction_of_data_to_send = fraction of data to send to other nodes

    **************************************************************************/

    private void handleChannel ( DlsClient client, StorageEngine channel,
           float fraction_of_data_to_send )
    {
        .log.info("Redistributing channel '{}'", channel.id);

        this.resources.bucket_iterator.setStorage(channel);
        this.resources.bucket_iterator.next(); // advance iterator to
                                               // the first record

        // simple statistics
        ulong buckets_sent = 0;
        ulong buckets_iterated = 0;

        auto current_node_index = 0;
        auto random_engine = new Random;

        while ( !this.resources.bucket_iterator.lastBucket )
        {
            if ( this.bucketShouldBeFowarded(random_engine,
                        fraction_of_data_to_send) )
            {
                this.forwardBucket(client, channel,
                        this.resources.bucket_iterator.bucket_first_key,
                        this.resources.bucket_iterator.file_path);

                buckets_sent++;
            }

            this.resources.bucket_iterator.next();

            buckets_iterated++;

            if ( buckets_iterated % 1_000 == 0 )
            {
                .log.trace("Progress redistributing channel '{}':  "
                           "{} buckets iterated, {} forwarded",
                           channel.id, buckets_iterated, buckets_sent);
            }

            // be nice
            this.resources.loop_ceder.handleCeding();
        }

        .log.trace("Finished redistributing channel '{}':  "
                   "{} buckets iterated, {} forwarded",
                   channel.id, buckets_iterated, buckets_sent);

        .log.trace("So far redistributed {} records with {} GB",
                this.records_redistributed, cast(double)(this.bytes_redistributed) / (1024 * 1024 * 1024));
    }


    /**************************************************************************

        Determiness wheter the bucket should be forwarded to the other node.
        It uses request's `fraction_data_to_send` and compares it to the
        random number in the (0, 1] interval.

        Params:
            rand = random number generator instance
            fraction_of_data_to_send = fraction of data to Redistribute

        Returns:
            true if the bucket should be forwarded, false otherwise.

    **************************************************************************/

    private bool bucketShouldBeFowarded ( Random rand,
            float fraction_of_data_to_send )
    {
        return fraction_of_data_to_send > rand.uniform!(float)();
    }

    /**************************************************************************

        Forwards a bucket to another node issuing `PutBatch` requests to the
        destination nodes. It puts record by record in the batch until the batch
        is full, it compresses and sends away the batch to one of the destination
        nodes.

        Params:
            client = DlsClient instance
            channel = StorageEngine instance which bucket belongs to
            first_record = first (theoretical) hash of the bucket
            bucket_path = path to the bucket.

    **************************************************************************/

    private void forwardBucket (DlsClient client, StorageEngine channel,
            hash_t first_record, cstring bucket_path)
    {
        // Rename the file to .tmp, making it inaccessible to the storage engine
        scope original_path = new FilePath(bucket_path);
        tmp_file_path.length = 0;
        enableStomping(tmp_file_path);
        sformat(tmp_file_path, "{}.tmp", bucket_path);
        original_path.rename(tmp_file_path);

        this.input_buffer.reset();
        this.input_buffer.length = 10 * 1024 * 1024;

        // Open the bucket for the reading
        scope file = new BucketFile(
                this.resources.async_io, this.resources.suspended_job,
                tmp_file_path, this.input_buffer[]);

        scope (exit) file.close();

        // Acquire a batcher
        auto batcher = this.resources.batcher();

        // Last read record header
        // Needs to be outside of prepareBatch as it is used to store
        // the postion of the last batched record.
        RecordHeader header;
        bool eof;

        do
        {
            batcher.clear();

            eof = this.prepareBatch(file, batcher, channel, header);

            this.sendBatchUntilSuccess(client, channel, batcher);

        } while (!eof);
    }

    /**************************************************************************

        Reads as much records from the file and packs it in the batch until
        batch is full or end of the file is reached.

        Params:
            file = file to read records from
            batcher = record batcher instance to use for packing
            channel = StorageEngine instance which bucket belongs to
            header = last read record header

        Returns:
            true if the end of file is reached

    ***************************************************************************/

    private bool prepareBatch (ref BucketFile file, RecordBatcher batcher,
            StorageEngine channel,
            ref RecordHeader header)
    {
        bool eof;
        RecordBatcher.AddResult batching_res;

        // Read values from the channel and pack them into the batch
        do
        {
            eof = file.nextRecord (this.resources.suspended_job, header);

            if (eof)
            {
                break;
            }

            this.resources.value_buffer().length = header.len;
            file.readRecordValue (this.resources.suspended_job,
                    header, *this.resources.value_buffer());
            this.resources.key_buffer().length = Hash.HexDigest.length;
            Hash.toHexString(header.key, *this.resources.key_buffer());

            batching_res = batcher.add(*this.resources.key_buffer(),
                *this.resources.value_buffer());


            switch (batching_res)
            {
                case RecordBatcher.AddResult.Added:
                    auto record_length = this.resources.value_buffer().length;

                    this.resources.node_info.record_action_counters
                        .increment("redistributed", record_length);

                    // Increase local statistics
                    this.bytes_redistributed += record_length;
                    this.records_redistributed++;

                    break;

                case RecordBatcher.AddResult.TooBig:
                    .log.warn("Channel {}: Record {} can't fit in the batch (size: {}), it will be discarded",
                            channel.id, header.key, header.len);
                    continue;

                default:
            }

            // be nice
            this.resources.loop_ceder.handleCeding();
        }
        while (batching_res == RecordBatcher.AddResult.Added);

        return eof;
    }

    /**************************************************************************

        Sends a batch (potentially to a different node on every try) until
        success.

        Params:
            client = DlsClient instance
            channel = StorageEngine instance which bucket belongs to
            batcher = record batcher instance to use for packing

    ***************************************************************************/

    void sendBatchUntilSuccess (DlsClient client, StorageEngine channel,
            RecordBatcher batcher)
    {
        // Indicator if the last PutBatch request was finished without errors
        bool error = false;

        // PutBatch notifier, will set the `error`.
        void notify ( DlsClient.RequestNotification info )
        {
            if (info.type == info.type.Finished)
            {
                if ( !info.succeeded )
                {
                    error = true;
                }
                else
                {
                    error = false;
                }
            }
        }

        do
        {
            RecordBatcher putBatchCb(DlsClient.RequestContext context) {
                return batcher;
            }

            // run in loop, on error this will reassign request to the another node.
            client.perform(this.reader.fiber, client.putBatch(channel.id,
                        &putBatchCb, &notify));

            if (error)
                .log.trace ("Repeating request due to error!");

        }
        while (error);
    }

    /***************************************************************************

        Temporary file path for the buckets that will be sent away. Note that,
        although this will prevent multiple allocations within a single request,
        this will cause a heap allocation every time this request is handled.

    ***************************************************************************/

    private mstring tmp_file_path;
}
