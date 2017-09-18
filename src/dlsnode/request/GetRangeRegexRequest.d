/*******************************************************************************

    GetRangeRegex request class.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.request.GetRangeRegexRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dlsproto.node.request.GetRangeRegex;
import ocean.text.regex.PCRE;

import ocean.util.log.Logger;
import ocean.transition;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dlsnode.request.GetRangeRegexRequest");
}

/*******************************************************************************

    GetRangeRegex request

*******************************************************************************/

scope class GetRangeRegexRequest : Protocol.GetRangeRegex
{
    import dlsnode.request.model.IterationMixin;
    import dlsnode.request.model.ConstructorMixin;

    import dlsproto.client.legacy.DlsConst;

    import ocean.text.Search;

    /***************************************************************************

        Sub-string search instance.

    ***************************************************************************/

    private SearchFruct!(Const!(char)) match;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Predicate that accepts records that match the specified filter

    ***************************************************************************/

    private bool filterPredicate ( cstring key, cstring value )
    {
        with ( DlsConst.FilterMode ) switch ( this.mode )
        {
            case StringMatch:
                return this.match.forward(value) < value.length;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    return this.resources.regex.match(value);
                }
                catch ( .PCRE.PcreException e )
                {
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and "
                        "key '{}': {} (error code: {}) @ {}:{} (aborting iteration)",
                        *this.pcre_filter, *this.resources.channel_buffer, key,
                        e.msg, e.error, e.file, e.line);

                    this.iterator.abort();
                    return false;
                }
                catch ( Exception e )
                {
                    .log.error("Exception during PCRE match with `{}` on channel '{}' and "
                        "key '{}': {} @ {}:{} (aborting iteration)",
                        *this.pcre_filter, *this.resources.channel_buffer,
                        key, e.msg, e.file, e.line);
                    this.iterator.abort();
                    return false;
                }
                assert(false);

            default:
                assert(false);
        }
        assert(false);
    }

    /***************************************************************************

        Adds this.iterator and prepareChannel override to initialize it
        Defines `getNext` that uses filterPredicate to filter records

    ***************************************************************************/

    mixin ChannelIteration!(resources, IterationKind.Ranged, filterPredicate);

    /***************************************************************************

        Initialized regex match based on provided filter string

        Params:
            mode = filter mode
            filter = filter string

        Returns:
            true if PCRE is valid and request can proceed, false otherwise

    ***************************************************************************/

    final override protected bool prepareFilter ( DlsConst.FilterMode mode,
        cstring filter )
    {
        with ( DlsConst.FilterMode ) switch ( this.mode )
        {
            case StringMatch:
                this.match = search(filter);
                break;

            case PCRE:
            case PCRECaseInsensitive:
                try
                {
                    auto case_sens = mode != PCRECaseInsensitive;
                    this.resources.regex.compile(filter, case_sens);
                }
                catch ( .PCRE.PcreException e )
                {
                    .log.warn("Exception during PCRE compile of `{}` on channel '{}': "
                        "{} (error code: {}) @ {}:{} (aborting iteration)",
                        *this.pcre_filter, *this.resources.channel_buffer,
                        e.msg, e.error, e.file, e.line);
                    return false;
                }
                catch ( Exception e )
                {
                    .log.warn("Exception during PCRE compile of `{}` on channel '{}': "
                        "{} @ {}:{} (aborting iteration)",
                        *this.pcre_filter, *this.resources.channel_buffer,
                        e.msg, e.file, e.line);
                    return false;
                }
                break;

            default:
                assert(false);
        }

        return true;
    }
}


version ( UnitTest )
{
    import ocean.core.Test;
    import ocean.text.regex.PCRE;
}


/*******************************************************************************

    Test PCRE matching over feasible strings / regexes.

*******************************************************************************/

unittest
{
    const logline = "http://somehost.example.org/flash/katze/param?&doc=hunde&linol=5555555999999955555&tropical=vogel&height=3&myid=445&oop=%YB%7X%22bb%22%3A%1216182355555555542744%22%2C%22fheight%22%3A22%7X%5X";
    auto pcre = new PCRE;

    // Positive match.
    // Searching for either:
    // "doc=hunde" followed by "tropical=vogel" or "tropical=vogel-motherille" or "tropical=vogel-bird"
    // OR
    // "tropical=vogel" or "tropical=vogel-motherille" or "tropical=vogel-bird" followed by "doc=hunde"
    test(pcre.preg_match(logline,
        "(doc=hunde.*(tropical=vogel|tropical=vogel-motherille|tropical=vogel-bird))|((tropical=vogel|tropical=vogel-motherille|tropical=vogel-bird).*doc=hunde)"
    ));

    // Failed match.
    // Searching in the same logline for either:
    // "doc=teuer" followed by "tropical=vogel" or "tropical=vogel-motherille" or "tropical=vogel-bird"
    // OR
    // "tropical=vogel" or "tropical=vogel-motherille" or "tropical=vogel-bird" followed by "doc=teuer"
    test(!pcre.preg_match(logline,
        "(doc=teuer.*(tropical=vogel|tropical=vogel-motherille|tropical=vogel-bird))|((tropical=vogel|tropical=vogel-motherille|tropical=vogel-bird).*doc=teuer)"
    ));
}
