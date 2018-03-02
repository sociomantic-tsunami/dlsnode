/******************************************************************************

    Tests the reading of the legacy buckets.

    Copyright: (c) 2016 Sociomantic Labs. All rights reserved.

******************************************************************************/

module integrationtest.versioning.cases.TestLegacyWrite;

import integrationtest.versioning.DlsVersioningCase;

import ocean.core.array.Search;
import ocean.transition;

/******************************************************************************

    Check if we can write to the legacy bucket.

*******************************************************************************/

class GetAllLegacy: DlsVersioningCase
{
    this ( )
    {
        this.test_channel = "putlegacy";
    }

    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "Put/GetAll over the legacy channel";
        return desc;
    }

    public override void run ( )
    {
        // A single bucket file in the legacy format is copied into the test DLS
        // node's data folder (see DlsVersioningRunner.copyFiles()). We can then
        // perform tests to check that the DLS can write and read it properly.

        // The layout of the legacy channel
        cstring[][hash_t] existing_records =
        [
            0x0000000057275806: ["Hello there"],
            0x0000000057275809: ["I'm a legacy channel"],
            0x000000005727580c: ["Nice to meet you finaly"],
            0x0000000057275810: ["Oh, yes indeed."]
        ];

        cstring[][hash_t] new_records = 
        [
            0x0000000057275813: ["test1"],
            0x0000000057275814: ["test2"],
            0x0000000057275815: ["test3"],
            0x0000000057275817: ["test4"]
        ];

        // Union of old and new records
        cstring[][hash_t] all_records =
        [
            0x0000000057275806: ["Hello there"],
            0x0000000057275809: ["I'm a legacy channel"],
            0x000000005727580c: ["Nice to meet you finaly"],
            0x0000000057275810: ["Oh, yes indeed."],
            0x0000000057275813: ["test1"],
            0x0000000057275814: ["test2"],
            0x0000000057275815: ["test3"],
            0x0000000057275817: ["test4"]
        ];

        foreach (k, v; new_records)
        {
            foreach (rec; v)
            {
                this.dls.put(this.test_channel, k, rec);
            }
        }
        
        // Do a GetAll to retrieve them all
        auto fetched = this.dls.getAll(this.test_channel);

        // Confirm the results
        test!("==")(fetched.length, all_records.length);
        bool[hash_t] checked;
        foreach (k, vals; fetched)
        {
            auto local_vals = k in all_records;
            test(local_vals.length == vals.length, "GetAll returned wrong key");

            foreach (val; vals)
            {
                test!("==")((*local_vals).contains(val), true);
            }

            test(!(k in checked), "GetAll returned the same key twice");
            checked[k] = true;
        }
    }
}
