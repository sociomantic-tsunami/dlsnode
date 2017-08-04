.. contents::
  :depth: 2

DLS Node
^^^^^^^^

Description
===========

The DLS node is a server which handles requests from the DLS client defined
in dlsproto (``dlsproto.dls.DlsClient``), via the DLS protocol. One or more nodes make
up a complete DLS, though only the client has this knowledge -- individual nodes
know nothing of each others' existence.

Data in the DLS node is stored on disk in a series of folders and files, based
on the channel and timestamp of each record stored.

Deployment
==========

Installing
----------

DLS node is deployed via Debian packages located on APT server, so the
installation is as simple as ``sudo apt-get install dlsnode-d1=version``. The
install process doesn't restart already running service, so use ``sudo service
dls restart`` to restart it.

Daemon
------

The DLS node runs as dlsnode user as a daemon. There's no controlling terminal
assigned to it.

Processes
---------

A single instance of the DLS node runs on each assigned server.
DLS' runs in ``/srv/dlsnode`` directory, and upstart script runs
``/srv/dlsnode/dlsnode`` which should be a symlink normally resolving to
``/usr/sbin/dlsnode-d1``.

Upstart
-------

The DLS node is configured to use upstart and will start automatically upon
server reboot. The upstart scripts are located in ``/etc/init/dls.conf``.

Manually
--------

To manually start the DLS nodes on a server, run ``sudo service dls
start``. This will start the DLS node process. If the
application session are already running, you can use ``sudo service dls restart``
to restart them.

Monitoring
==========

Resource Usage
--------------

A DLS node spawns several dozens of threads, where each typically requires very
little CPU time, perhaps 10%. Note that all threads are sharing the same
virtual address space, so they all should use the same amount of RAM (usually a
few hundred MBs).  Anything beyond this might indicate a problem.

Checking Everything's OK
------------------------

Log Files
.........

The DLS node writes two log files:

``root.log``
  Notification of errors when handling requests.

``stats.log``
  Statistics about the number of records and bytes stored (globally and per
  channel), the number of bytes sent and received over the network, and the
  number of open connections and records handled.

Possible Problems
-----------------

Crash
.....

Many applications in the system rely on being able to read and/or write to the
DLS. If a single DLS node goes down, an equivalent proportion of requests from
client applications will fail. There is currently no fall-back mechanism, beyond
the possibility for the client applications themselves to cache and retry failed
requests. The system is, at this stage, pretty robust; all client applications
can handle the situation where a DLS node is inaccessible and reconnect safely
when it returns.

If a DLS node crashes, it can simply be restarted.

Data Corruption
...............

There have been instances in the past where data for a channel has become
corrupt. This usually happens when a DLS node fails to cleanly shut down,
either due to a crash or a server reboot. In this case, data which was in the
process of being written may have actually only been partly written to disk,
resulting in invalid data in one or more block files.

This problem is usually not critical, and the DLS node will continue to
function normally. Previously, the data returned by various iteration commands
will simply be truncated, but as of v1.6.0, DLS node is truncating all the
potentially corrupted files for which it doesn't know if the data is corrupted
or not.

There is a script in the ``util`` folder which was used to  parse the DLS data
format, check for errors, and (optionally) fix them by truncating any
subsequent data in the file after the point where an error is found. This is
normally no longer needed, as the DLS node should truncate the data itself.

Design
======

The structure of the DLS node's code is based very closely around the structure
of the ``core.node`` package of swarm.

The basic components are:

Select Listener
  The ``swarm.core.node.model.Node : NodeBase`` class, which forms the
  foundation of all swarm nodes, owns an instance of
  ``ocean.net.server.SelectListener : SelectListener``. This provides the basic
  functionality of a server; that is, a listening socket which will accept
  incoming client connections. Each client connection is assigned to a
  connection handler instance from a pool.

Connection Handler Pool
  The select listener manages a pool of connection handlers (derived from
  ``swarm.core.node.connection.ConnectionHandler : ConnectionHandlerTemplate``.
  Each is associated with an incoming socket connection from a client. The
  connection handler reads a request code from the socket and then passes the
  request on to a request handler instance, which is constructed at scope (i.e.
  only exists for the lifetime of the request).

Request Handlers
  A handler class exists for each type of request which the node can handle.
  These are derived from ``swarm.core.node.request.model.IRequest : IRequest``.
  The request handler performs all communication with the client which is
  required by the protocol for the given request. This usually involves
  interacting with the node's storage channels.

Storage Channels
  The ``swarm.core.node.storage.model.IStorageChannels : IStorageChannelsTemplate``
  class provides the base for a set of storage channels, where each channel is
  conceived as storing a different type of data in the system. The individual
  storage channels are derived from
  ``swarm.core.node.storage.model.IStorageEngine : IStorageEngine``.

Threads
  The ``dlsnode.util.aio.*`` package is a separate part of DLS node which
  spawns ``Performance.number_of_thread_workers`` (set in ``config.ini``)
  to perform the disk IO in the separate threads, thus not blocking the hot path
  waiting on disk IO. This results in ``htop`` showing several dozens of
  ``dlsnode`` rows, one for each thread.

Checkpoint file
  The DLS node continuously writes bucket files containing incoming data, followed
  by regular ``fsync`` calls, making sure that all data is synced to disk.
  Regular ``fsync`` calls are implemented as part of ``CheckpointService`` (found
  in ``dlsnode.storage.checkpoint.CheckpointService``). Checkpoint service wakes
  up every few minutes (configurable as part of
  ``Performance.checkpoint_commit_seconds``), performs ``fsync`` for every open
  bucket and stores the last fsynced position in ``data/checkpoint.dat`` file -
  one line per Bucket (containing channel name, the first bucket timestamp and the
  last fsynced position in bucket). On normal exit, this file is deleted. After DLS
  is restarted after crash it will find this file, and it will truncate every bucket
  to the last known good position, throwing out the data that could potentially be
  corrupted by crash.

Data Flow
=========

DLS nodes do not access any other data stores.

Dependencies
============

:Dependency: liblzo2
