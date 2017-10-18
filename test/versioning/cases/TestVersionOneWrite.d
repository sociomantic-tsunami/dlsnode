/******************************************************************************

    Tests the reading of the version one buckets.

    Copyright: (c) 2016 Sociomantic Labs. All rights reserved.

******************************************************************************/

module test.versioning.cases.TestVersionOneWrite;

import test.versioning.DlsVersioningCase;

import ocean.core.array.Search;
import ocean.transition;

/******************************************************************************

    Check if we can write to the version one bucket.

*******************************************************************************/

class GetAllLegacy: DlsVersioningCase
{
    this ( )
    {
        this.test_channel = "putversionone";
    }

    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "Put/GetAll over the new channel";
        return desc;
    }

    public override void run ( )
    {
        // A single bucket file in the new format is copied into the test DLS
        // node's data folder (see DlsVersioningRunner.copyFiles()). We can then
        // perform tests to check that the DLS can write and read it properly.

        // The layout of the legacy channel
        cstring[][hash_t] existing_records =
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

        cstring[][hash_t] new_records = 
        [
            0x0000000057275476: ["test1"],
            0x0000000057275477: ["test2"],
            0x0000000057275478: ["test3"],
            0x0000000057275479: ["test4"]
        ];

        // Union of old and new records
        cstring[][hash_t] all_records =
        [
            0x000000005727545c: ["Hello there"],
            0x0000000057275461: ["how are you are you fine"],
            0x0000000057275464: ["I'm very good! Thanks!"],
            0x000000005727546a: ["Let's see"],
            0x000000005727546d: ["This one will get broken"],
            0x0000000057275471: ["we'll never receive this one"],
            0x0000000057275474: ["nor this one"],
            0x0000000057275475: ["bye"],
            0x0000000057275476: ["test1"],
            0x0000000057275477: ["test2"],
            0x0000000057275478: ["test3"],
            0x0000000057275479: ["test4"]
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
