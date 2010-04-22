package org.neo4j.graphdb.traversal;

public enum Uniqueness
{
    /**
     * This is what the legacy traversal framework does. A node cannot be
     * traversed more than once.
     */
    NODE_GLOBAL,
    /**
     * For each returned node there's a path from the start node to it. Any such
     * path is guaranteed to be unique.
     */
    NODE_PATH,
    /**
     * This is like "globally unique", but with the following difference:
     * Traversing a huge graph is quite memory intense in that it keeps track of
     * ALL the nodes it has visited. For super huge graphs a traverser can hog
     * all the memory in the JVM, causing OOM exception. With this you can
     * supply a count, which is the number of most recent visited nodes. This
     * can cause a node to be visited more than once, but scales infinitely.
     */
    NODE_RECENT,
    /**
     * A relationships cannot be traversed more than once, whereas nodes can.
     */
    RELATIONSHIP_GLOBAL,
    /**
     * For each returned node there's a path from the start node to it. Any such
     * path is guaranteed to be unique (relationship wise).
     */
    RELATIONSHIP_PATH,
    /**
     * Same as the node version, but for relationships.
     */
    RELATIONSHIP_RECENT,
    /** No restriction (user will have to manage it) */
    NONE;
}
