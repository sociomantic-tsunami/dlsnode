/*******************************************************************************

    DLS Node Server

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.versioning.DlsVersioningCase;

import dlstest.DlsTestCase;

/******************************************************************************

    DlsVersioning test cases. Uses the client that connects to the 11_000
    port.

*******************************************************************************/

class DlsVersioningCase: DlsTestCase
{
    /***************************************************************************

        Creates new DLS client for a test case and proceeds with connect so
        that client instance will be ready to work by the time `run` methods
        is being run. This differs from the proto's DlsTestCase as it uses
        different port (to be able to run tests simultaneously).

    ***************************************************************************/

    override public void prepare ( )
    {
        this.dls = new DlsClient();
        this.dls.addNode(11_000);
        this.dls.connect(this.protocol_type);
    }
}
