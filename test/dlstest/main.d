/*******************************************************************************

    DLS node test runner

    Imports the DLS test from dlsproto and runs it on the real DLS node.

    copyright: Copyright (c) 2016 sociomantic labs. All rights reserved

*******************************************************************************/

module dlstest.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import dlstest.TestRunner;
import turtle.runner.Runner;

/*******************************************************************************

    Test runner which spawns a real DLS node to run tests on.

*******************************************************************************/

private class RealDlsTestRunner : DlsTestRunner
{
    /***************************************************************************

        Copies the DLS node's config file to the sandbox before starting the
        node.

    ***************************************************************************/

    override public CopyFileEntry[] copyFiles ( )
    {
        return [
            CopyFileEntry("/test/dlstest/etc/config.ini", "etc/config.ini")
        ];
    }


    /***************************************************************************

        Disables test cases that are very slow on this particular implementation
        of the storage engine.

        Returns:
            an array of fully-qualified class names for test cases that must
            be ignored by this test runner despite being compiled in.

    ***************************************************************************/

    override istring[] disabledTestCases ( )
    {
        return [ "dlstest.cases.UnorderedMultiplePut.UnorderedMultiPutTest",
                 "dlstest.cases.UnorderedPut.UnorderedPutTest",
                 "dlstest.cases.neo.UnorderedMultiplePut.UnorderedMultiPutTest",
                 "dlstest.cases.neo.UnorderedPut.UnorderedPutTest" ];

    }

}

/*******************************************************************************

    Main function. Forwards arguments to test runner.

*******************************************************************************/

int main ( istring[] args )
{
    auto runner =
        new TurtleRunner!(RealDlsTestRunner)("dlsnode", "dlstest.cases.legacy");
    return runner.main(args);
}
