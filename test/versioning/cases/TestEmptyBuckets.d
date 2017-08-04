/******************************************************************************

    Test reading a channel with empty buckets inside

    Copyright: (c) 2016 Sociomantic Labs. All rights reserved.

******************************************************************************/

module test.versioning.cases.TestEmptyBuckets;

import dlstest.DlsTestCase;

import ocean.core.array.Search;
import ocean.transition;

/******************************************************************************

    Test if the iteration through the channel works even if some of the buckets
    are empty.

*******************************************************************************/

class GetAllEmptyBuckets: DlsTestCase
{
    this ( )
    {
        this.test_channel = "empty-bucket";
    }

    override public Description description ( )
    {
        Description desc;
        desc.priority = 100;
        desc.name = "GetAll over the channel which includes empty buckets";
        return desc;
    }

    public override void run ( )
    {
        // Do a GetAll to retrieve them all
        auto fetched = this.dls.getAll(this.test_channel);

        // Confirm the results (for this test, just the count would do fine)
        test!("==")(fetched.length, 8);
    }
}
