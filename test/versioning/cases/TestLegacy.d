/******************************************************************************

    Tests the reading of the legacy buckets.

    Copyright: (c) 2016 Sociomantic Labs. All rights reserved.

******************************************************************************/

module test.versioning.cases.TestLegacy;

import dlstest.DlsTestCase;

import ocean.core.array.Search;
import ocean.transition;

/******************************************************************************

    Check if the legacy records can be read.

*******************************************************************************/

class GetAllLegacy: DlsTestCase
{
    this ( )
    {
        this.test_channel = "legacy";
    }

    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetAll over the legacy channel";
        return desc;
    }

    public override void run ( )
    {
        // A single bucket file in the legacy format is copied into the test DLS
        // node's data folder (see DlsVersioningRunner.copyFiles()). We can then
        // perform tests to check that the DLS can read it properly.

        // The layout of the legacy channel
        cstring[][hash_t] records =
        [
            0x0000000057275806: ["Hello there"],
            0x0000000057275809: ["I'm a legacy channel"],
            0x000000005727580c: ["Nice to meet you finaly"],
            0x0000000057275810: ["Oh, yes indeed."]
        ];

        // Do a GetAll to retrieve them all
        auto fetched = this.dls.getAll(this.test_channel);

        // Confirm the results
        test!("==")(fetched.length, records.length);
        bool[hash_t] checked;
        foreach (k, vals; fetched)
        {
            auto local_vals = k in records;
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
