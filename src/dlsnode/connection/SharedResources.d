/*******************************************************************************

    DLS node shared resource manager. Handles acquiring / relinquishing of
    global resources by active request handlers.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.connection.SharedResources;



/*******************************************************************************

    Imports

    Imports which are required by the ConnectionResources struct, below, are
    imported publicly, as they are also needed in
    dlsnode.request.model.RequestResources (which imports this
    module). This is done to simplify the process of modifying the fields of
    ConnectionResources --  forgetting to import something into both modules
    is a common source of very confusing compile errors.

*******************************************************************************/

import swarm.common.connection.ISharedResources;

public import ocean.io.select.client.FiberSelectEvent;

public import dlsnode.util.aio.EventFDJobNotification;

public import ocean.text.regex.PCRE;

public import swarm.common.request.helper.LoopCeder;

public import dlsnode.storage.iterator.StorageEngineStepIterator;

public import dlsnode.storage.iterator.StorageEngineFileIterator;

public import swarm.util.RecordBatcher;

public import swarm.Const: NodeItem;

public import dlsnode.connection.client.DlsClient;

/*******************************************************************************

    Compiled regex alias, required by SharedResources_T template, which
    apparently can't properly resolve nested types.

*******************************************************************************/

public alias PCRE.CompiledRegex CompiledRegex;



/*******************************************************************************

    Struct whose fields define the set of shared resources which can be acquired
    by a request. Each request can acquire a single instance of each field.

*******************************************************************************/

public struct ConnectionResources
{
    import ocean.meta.types.Qualifiers : cstring, mstring;

    mstring channel_buffer;
    mstring key_buffer;
    mstring key2_buffer;
    mstring filter_buffer;
    mstring batch_buffer;
    mstring value_buffer;
    ubyte[] putbatch_compress_buffer;
    mstring bucket_path_buffer;
    cstring[] channel_list_buffer;
    hash_t[] hash_buffer;
    ubyte[] record_buffer;
    FiberSelectEvent event;
    EventFDJobNotification suspended_job;
    LoopCeder loop_ceder;
    StorageEngineStepIterator iterator;
    StorageEngineFileIterator bucket_iterator;
    RecordBatcher batcher;
    RecordBatch record_batch;
    RecordBatch decompress_record_batch;
    CompiledRegex regex;
    NodeItem[] redistribute_node_buffer;
    DlsClient dls_client;
}



/*******************************************************************************

    Mix in a class called SharedResources which contains a free list for each of
    the fields of ConnectionResources. The free lists are used by
    individual requests to acquire and relinquish resources required for
    handling.

*******************************************************************************/

mixin SharedResources_T!(ConnectionResources);

