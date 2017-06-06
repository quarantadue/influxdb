# Query Engine Internals

The query engine contains the processing framework for reading data
from the storage engine and processing it to produce data.

## Overview

The lifecycle of a query looks like this:

1. The InfluxQL query string is tokenized and then parsed into an abstract syntax
   tree (AST). This is the code representation of the query itself.
2. The query AST is compiled into a preliminary directed acyclical graph by
   the compiler. The query itself is validated and a directed acyclical graph
   is created for each field within the query. Any symbols (variables) that
   are encountered are recorded and will be resolved in the next stage. This
   stage is isolated and does not require any information from the storage
   engine or meta client.
3. The shard mapper is used to retrieve the shards from the storage engine that
   will be used by the query. Symbols are resolved to a real data type, wildcards
   are expanded, and type validation happens. This stage requires access to the
   storage engine and meta client. The full directed acyclical graph is constructed
   to be executed in the future.
4. The directed acyclical graph is processed. Each node processes its dependencies
   and then executes the creation of its own iterator using those dependencies.
5. The iterators for the leaf edges are processed as the outputs of the query.
   The emitter aggregates these points, matches them by time/name/tags, and then
   creates rows that get sent to the httpd service to be processed in whichever way
   is desirable.

## Nodes and Edges

The directed acyclical graph is composed of nodes and edges. Each node
represents some discrete action to be done on the input iterators.
Commonly, this means creating a new iterator to read and process the
inputs.

Each node can have zero or more inputs and at least one output. Nodes
are passed around as an interface.

An edge is composed of two parts: the read and write edge. The read edge
is the part of the edge meant to be used for reading the iterator that
was produced by a node. The write edge is the end where a node writes
to an iterator. So if you were to draw it as a directed acyclical graph,
you would end up with something like this:

    write ------> read

An edge is always composed of a write and read edge. Each side of the
edge will usually be associated with a node. A read edge at the end of
the graph (a leaf node) will not be associated with a node, but will
just be read from directly.

The read and write edge are split into two separate components to make it
easier to manipulate the graph. When connecting an edge to a node, an
attribute has to be set on both the node and the edge. If we were to move
an edge to point to a different node, we would have to modify the underlying
node for both sides of the edge. Since the edges are separate, we do not
need to modify the other side of the edge to modify an edge when we
insert, append, or remove an edge related to another node.

## Optimization

Optimization is done by manipulating the directed acyclical graph.
At first, a query plan will construct the simplest method of resolving
the query. For example, in the following query we might see:

    > SELECT count(value) FROM cpu
    shard [1] -\
    shard [2] ----> merge --> count()
    shard [3] -/

We can then optimize this merge. We know that `count()` can be used as
a partial aggregate. We can insert a `count()` function call between
each shard and the merge node and then modify the original `count()`
to be a `sum()`. The final query plan would look like this:

    shard [1] --> count() --\
    shard [2] --> count() ----> merge --> sum()
    shard [3] --> count() --/

Once we have generated the graph, we pass it to the executor to create
the iterators so the data can be processed.
