/******************************************************************************

    Tests the reading of the buckets with parity check with no errors inside.

    Copyright: (c) 2016 dunnhumby Germany GmbH. All rights reserved.

******************************************************************************/

module integrationtest.versioning.cases.TestParityFine;

import integrationtest.versioning.DlsVersioningCase;

import ocean.core.array.Search;
import ocean.transition;

/******************************************************************************

    Tests if the buckets with parity check can be read

*******************************************************************************/

class GetAllParityFine: DlsVersioningCase
{
    this ( )
    {
        this.test_channel = "parity-fine";
    }

    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetAll over the good channel with parity check";
        return desc;
    }

    public override void run ( )
    {
        // A single bucket file in the v1 format is copied into the test DLS
        // node's data folder (see DlsVersioningRunner.copyFiles()). We can then
        // perform tests to check that the DLS can read it properly.

        // The layout of the test channel
        // (Despite what some of the record values say, all should get read
        // successfully.)
        cstring[][hash_t] records =
        [
            0x000000005727545c: ["Hello there"],
            0x0000000057275461: ["how are you are you fine"],
            0x0000000057275464: ["I'm very good! Thanks!"],
            0x000000005727546a: ["Let's see"],
            0x000000005727546d: ["This one will get broken"],
            0x0000000057275471: ["we'll never receive this one"],
            0x0000000057275474: ["nor this one"],
            0x0000000057275475: ["bye"]
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
