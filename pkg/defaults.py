def get_desc(extra=''):
    if extra:
        extra = ' (' + extra + ')'
    return 'Distributed loglines storage node' + extra + '''

The dls node is a server which handles requests from the dls client defined in
swarm (swarm.dht.DlsClient). One or more nodes make up a complete dls, though
only the client has this knowledge -- individual nodes know nothing of each
others' existence.

Data in the dls node is stored in file system, under the data directory, with
a separate subdirectory per data channel.'''

OPTS = dict(
    name = VAR.name,
    url = 'https://github.com/sociomantic-tsunami/dlsnode',
    maintainer = 'Sociomantic Labs GmbH <tsunami@sociomantic.com>',
    vendor = 'Sociomantic Labs GmbH',
    description = get_desc(),
    category = 'net'
)


# vim: set ft=python et sw=4 sts=4 :
