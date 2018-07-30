/*******************************************************************************

    Performance config class for use with ocean.util.config.ConfigFiller.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.app.config.PerformanceConfig;



/*******************************************************************************

    Performance config values

*******************************************************************************/

public class PerformanceConfig
{
    uint write_flush_ms = 250;

    /**************************************************************************

        Size of the buffer used for buffering file reads. Setting this value
        to 0 turns off the buffering.

    **************************************************************************/

    size_t file_buffer_size = 10 * 1024 * 1024;

    /**************************************************************************

        Time interval between commits to
        checkpoint log

    **************************************************************************/

    uint checkpoint_commit_seconds = 300;

    /**************************************************************************

        Number of thread workers for the AsyncIO

    **************************************************************************/

    uint number_of_thread_workers = 20;
}

