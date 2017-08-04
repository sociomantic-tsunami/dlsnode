/*******************************************************************************

    Server config class for use with ocean.util.config.ConfigFiller.

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.app.config.ServerConfig;

import ocean.transition;


/*******************************************************************************

    Imports

*******************************************************************************/

import ConfigReader = ocean.util.config.ConfigFiller;



/*******************************************************************************

    Server config values

*******************************************************************************/

public class ServerConfig
{
    ConfigReader.Required!(mstring) address;

    ConfigReader.Required!(ushort) port;

    /// CPU index counting from 0; negative: use any CPU
    ConfigReader.Min!(int, -1) cpu;

    mstring data_dir = "data".dup;

    uint connection_limit = 5_000;

    uint backlog = 2048;
}

