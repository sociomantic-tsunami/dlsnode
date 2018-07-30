/*******************************************************************************

    Mixin for shared request initialization code

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.model.ConstructorMixin;

/*******************************************************************************

    Common code shared by all requests after the protocol split (which
    requires storing reference to node-specific shared resource object)

*******************************************************************************/

public template RequestConstruction ( )
{
    import dlsnode.request.model.RequestResources;

    /***************************************************************************

        Keeps resource object without reducing it to DlsCommand.Resources
        interface

    ***************************************************************************/

    private IDlsRequestResources resources;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDlsRequestResources resources )
    {
        super(reader, writer, resources);
        this.resources = resources;
    }
}
