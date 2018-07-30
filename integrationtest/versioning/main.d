/*******************************************************************************

    Dls versioning system test. Runs a set of test for the versioning
    system.

    copyright: Copyright (c) 2016 dunnhumby Germany GmbH. All rights reserved

*******************************************************************************/

module integrationtest.versioning.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import turtle.runner.Runner;

import integrationtest.versioning.cases.TestEmptyBuckets;
import integrationtest.versioning.cases.TestLegacy;
import integrationtest.versioning.cases.TestLegacyWrite;
import integrationtest.versioning.cases.TestVersionOneWrite;
import integrationtest.versioning.cases.TestParityFine;
import integrationtest.versioning.cases.TestParityBroken;


/*******************************************************************************

    Test runner which spawns a real DLS node and copies data directories.

*******************************************************************************/

private class DlsVersioningRunner : TurtleRunnerTask!(TestedAppKind.Daemon)
{
    /***************************************************************************

        No arguments but add small startup delay to let DLS node initialize
        listening socket.

    ***************************************************************************/

    override protected void configureTestedApplication ( out double delay,
        out istring[] args, out istring[istring] env )
    {
        delay = 1.0;
        args  = null;
        env   = null;
    }


    /***************************************************************************

        Copies the DLS node's config file to the sandbox before starting the
        node.

    ***************************************************************************/

    override public CopyFileEntry[] copyFiles ( )
    {
        return [
            CopyFileEntry("/integrationtest/versioning/etc/config.ini", "etc/config.ini"),
            CopyFileEntry("/integrationtest/dlstest/etc/credentials", "etc/credentials"),
            CopyFileEntry("/integrationtest/versioning/data/putlegacy/0000000057/275",
                    "data/putlegacy/0000000057/275"),
            CopyFileEntry("/integrationtest/versioning/data/parity-fine/0000000057/275",
                    "data/putversionone/0000000057/275"),
            CopyFileEntry("/integrationtest/versioning/data/legacy/0000000057/275",
                    "data/legacy/0000000057/275"),
            CopyFileEntry("/integrationtest/versioning/data/parity-fine/0000000057/275",
                    "data/parity-fine/0000000057/275"),
            CopyFileEntry("/integrationtest/versioning/data/parity-broken/0000000057/275",
                    "data/parity-broken/0000000057/275"),
            CopyFileEntry("/integrationtest/versioning/data/legacy/0000000057/275",
                    "data/mixed/0000000057/275"),
            CopyFileEntry("/integrationtest/versioning/data/parity-fine/0000000057/275",
                    "data/mixed/0000000058/275"),
            CopyFileEntry("/integrationtest/versioning/data/empty-bucket/0000000057/21b",
                    "data/empty-bucket/0000000057/21b"),
            CopyFileEntry("/integrationtest/versioning/data/empty-bucket/0000000057/21c",
                    "data/empty-bucket/0000000057/21c"),
            CopyFileEntry("/integrationtest/versioning/data/empty-bucket/0000000057/220",
                    "data/empty-bucket/0000000057/220"),
            CopyFileEntry("/integrationtest/versioning/data/empty-bucket/0000000057/236",
                    "data/empty-bucket/0000000057/236"),
            CopyFileEntry("/integrationtest/versioning/data/empty-bucket/0000000057/25b",
                    "data/empty-bucket/0000000057/25b"),
            CopyFileEntry("/integrationtest/versioning/data/empty-bucket/0000000057/6bb",
                    "data/empty-bucket/0000000057/6bb"),
            CopyFileEntry("/integrationtest/versioning/data/empty-bucket/0000000057/6bc",
                    "data/empty-bucket/0000000057/6bc")
        ];
    }


    /***************************************************************************

        No additional configuration necessary, assume localhost and
        hard-coded port number (10000)

    ***************************************************************************/

    override public void prepare ( ) { }
}

/*******************************************************************************

    Main function. Forwards arguments to test runner.

*******************************************************************************/

version (UnitTest) {} else
int main ( istring[] args )
{
    auto runner =
        new TurtleRunner!(DlsVersioningRunner)("dlsnode", "integrationtest.versioning.cases");
    return runner.main(args);
}
