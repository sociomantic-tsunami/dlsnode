/*******************************************************************************

    Module containing record layout.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

********************************************************************************/

module dlsnode.storage.Record;

import ocean.core.Traits;

/*******************************************************************************

    Record header struct definition; contains the key and the value length
    of a record

*******************************************************************************/

public align (1) struct RecordHeader
{
    hash_t key;
    size_t len;
}


/*******************************************************************************

    V1 record header, contains parity checksum

*******************************************************************************/

public align (1) struct RecordHeaderV1
{
    RecordHeader header;

    /// Horizontal parity.
    ubyte parity;


    /***************************************************************************

        Calculates the parity of the serialised data of this instance including
        this.parity. Does not modify this.parity.

        Returns:
            The parity remainder of all data of this instance, including
            this.parity.

    ***************************************************************************/

    public ubyte calcParity ( )
    {
        ulong parity = 0;

        // Calculate the parity of all fields in this instance. The fields in
        // this instance are expected to be either integers or structs containing
        // only integer fields. "Integer" includes bool here (which
        // ocean.core.Traits_tango.isIntegralType() excludes).
        foreach (x; this.tupleof)
        {
            static if (is(typeof(x) == struct))
            {
                foreach (y; x.tupleof)
                {
                    static assert(isPrimitiveType!(typeof(y)));
                    parity ^= y; // compiles only if y is an integer or bool
                }
            }
            else
            {
                static assert(isPrimitiveType!(typeof(x)));
                parity ^= x; // compiles only if x is an integer or bool
            }
        }

        parity ^= parity >> 32;
        parity ^= parity >> 16;
        parity ^= parity >> 8;

        return cast(ubyte)parity;
    }
}
