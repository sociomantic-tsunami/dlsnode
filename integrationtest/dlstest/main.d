/*******************************************************************************

    DLS node test runner

    Imports the DLS test from dlsproto and runs it on the real DLS node.

    copyright: Copyright (c) 2016 dunnhumby Germany GmbH. All rights reserved

*******************************************************************************/

module integrationtest.dlstest.main;

/*******************************************************************************

    Imports

*******************************************************************************/

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
            CopyFileEntry("/integrationtest/dlstest/etc/config.ini", "etc/config.ini"),
            CopyFileEntry("/integrationtest/dlstest/etc/credentials", "etc/credentials")
        ];
    }


    /***************************************************************************

        Disables test cases that are very slow on this particular implementation
        of the storage engine.

        Returns:
            an array of fully-qualified class names for test cases that must
            be ignored by this test runner despite being compiled in.

    ***************************************************************************/

    override string[] disabledTestCases ( )
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

version (UnitTest) {} else
int main ( string[] args )
{
    auto runner =
        new TurtleRunner!(RealDlsTestRunner)("dlsnode", "dlstest.cases");
    return runner.main(args);
}
