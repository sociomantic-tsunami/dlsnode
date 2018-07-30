/******************************************************************************

    LogRecord LRU cache. This cache behaves like a pool, on dropping the old item,
    data inside it is commited and the record is reused.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

******************************************************************************/

module dlsnode.storage.util.RecentFiles;

import dlsnode.util.aio.JobNotification;
import ocean.util.container.cache.LRUCache;

import dlsnode.storage.BufferedBucketOutput;

class RecentFiles: LRUCache!(BufferedBucketOutput)
{
    /***************************************************************************

        Constructor.

        Params:
            max_items = maximum number of items in the cache, set once, cannot
                be changed

    ***************************************************************************/

    public this ( size_t max_items )
    {
        super(max_items);
    }

    /**************************************************************************

        Acquires an existing bucket from cache, or creates a new one, and drops
        an old one. Because on dropping old one sync might be required, the
        JobNotification of the current request needs to be passed, which
        is the reason why this method accepts suspended_job - it merely passes it
        to `itemDropped` (indirecly, since that method is called by the base
        class).

        Params:
            key = key of the cache entry to get or create
            existed = will be set to true if the bucket already existed

        Returns:
            pointer to the BufferedBucketOutput slot in the cache

    **************************************************************************/

    public override BufferedBucketOutput* getRefreshOrCreate ( hash_t key, out bool existed)
    {
            return super.getRefreshOrCreate(key, existed);
    }

    /***************************************************************************

        A notifier which is fired when an item is removed from the cache.

        The notifier is called after the item has already been removed.
        The default implementation of the notifier inits the value of the item
        to remove any references to it.

        This override simply keeps reference stored, just it commits any changes
        that were not flushed, so we can just reuse the same object.

        Params:
            key = the key of the dropped item
            value = the dropped item

    ***************************************************************************/

    protected override void itemDropped (hash_t key, ref Value value)
    {
        if (value.value)
            value.value.commitAndClose();

    }
}
