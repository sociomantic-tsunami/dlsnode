/*******************************************************************************

    Dls versioning system test. Runs a set of test for the versioning
    system.

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

*******************************************************************************/

module test.versioning.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import turtle.runner.Runner;

import test.versioning.cases.TestEmptyBuckets;
import test.versioning.cases.TestLegacy;
import test.versioning.cases.TestLegacyWrite;
import test.versioning.cases.TestVersionOneWrite;
import test.versioning.cases.TestParityFine;
import test.versioning.cases.TestParityBroken;


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
            CopyFileEntry("/test/versioning/etc/config.ini", "etc/config.ini"),
            CopyFileEntry("/test/dlstest/etc/credentials", "etc/credentials"),
            CopyFileEntry("/test/versioning/data/putlegacy/0000000057/275",
                    "data/putlegacy/0000000057/275"),
            CopyFileEntry("/test/versioning/data/parity-fine/0000000057/275",
                    "data/putversionone/0000000057/275"),
            CopyFileEntry("/test/versioning/data/legacy/0000000057/275",
                    "data/legacy/0000000057/275"),
            CopyFileEntry("/test/versioning/data/parity-fine/0000000057/275",
                    "data/parity-fine/0000000057/275"),
            CopyFileEntry("/test/versioning/data/parity-broken/0000000057/275",
                    "data/parity-broken/0000000057/275"),
            CopyFileEntry("/test/versioning/data/legacy/0000000057/275",
                    "data/mixed/0000000057/275"),
            CopyFileEntry("/test/versioning/data/parity-fine/0000000057/275",
                    "data/mixed/0000000058/275"),
            CopyFileEntry("/test/versioning/data/empty-bucket/0000000057/21b",
                    "data/empty-bucket/0000000057/21b"),
            CopyFileEntry("/test/versioning/data/empty-bucket/0000000057/21c",
                    "data/empty-bucket/0000000057/21c"),
            CopyFileEntry("/test/versioning/data/empty-bucket/0000000057/220",
                    "data/empty-bucket/0000000057/220"),
            CopyFileEntry("/test/versioning/data/empty-bucket/0000000057/236",
                    "data/empty-bucket/0000000057/236"),
            CopyFileEntry("/test/versioning/data/empty-bucket/0000000057/25b",
                    "data/empty-bucket/0000000057/25b"),
            CopyFileEntry("/test/versioning/data/empty-bucket/0000000057/6bb",
                    "data/empty-bucket/0000000057/6bb"),
            CopyFileEntry("/test/versioning/data/empty-bucket/0000000057/6bc",
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

int main ( istring[] args )
{
    auto runner = new TurtleRunner!(DlsVersioningRunner)("dlsnode",
            "test.versioning.cases", "versioning_test");
    return runner.main(args);
}
