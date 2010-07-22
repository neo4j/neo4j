package org.neo4j.graphdb.traversal;

/**
 * Represents the type of uniqueness to expect from a traversal, for example the
 * uniqueness of nodes or relationships to visit during a traversal.
 */
public enum Uniqueness
{
    /**
     * A node cannot be traversed more than once. This is what the legacy
     * traversal framework does.
     */
    NODE_GLOBAL,
    /**
     * For each returned node there's a unique path from the start node to it.
     */
    NODE_PATH,
    /**
     * This is like {@link Uniqueness#NODE_GLOBAL}, but only guarantees
     * uniqueness among the most recent visited nodes, with a configurable
     * count. Traversing a huge graph is quite memory intensive in that it keeps
     * track of <i>all</i> the nodes it has visited. For huge graphs a traverser
     * can hog all the memory in the JVM, causing {@link OutOfMemoryError}.
     * Together with this {@link Uniqueness} you can supply a count, which is
     * the number of most recent visited nodes. This can cause a node to be
     * visited more than once, but scales infinitely.
     */
    NODE_RECENT,
    /**
     * A relationship cannot be traversed more than once, whereas nodes can.
     */
    RELATIONSHIP_GLOBAL,
    /**
     * For each returned node there's a (relationship wise) unique path from the
     * start node to it.
     */
    RELATIONSHIP_PATH,
    /**
     * Same as for {@link Uniqueness#NODE_RECENT}, but for relationships.
     */
    RELATIONSHIP_RECENT,
    /**
     * No restriction (the user will have to manage it).
     */
    NONE;
}
