package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

/**
 * Represents a position in a traversal. It has a {@link #node()},
 * {@link #depth()} from the start node and the {@link #lastRelationship()}
 * traversed. It also contains a {@link #path()} from the start node to here.
 */
public interface Position
{
    /**
     * The path from the start node of the traversal to this position.
     * @return the path to here from the start node.
     */
    Path path();

    /**
     * The node at this position.
     * @return the node at this position.
     */
    Node node();

    /**
     * The depth from the start node to here, i.e. the length of the
     * {@link #path()}.
     * @return the depth from the start node of the traversal to here.
     */
    int depth();

    /**
     * The last relationships traversed to get to this position.
     * @return the last relationship traversed to get here.
     */
    Relationship lastRelationship();

    /**
     * Returns {@code true} if, and only if the traversal is at the start node
     * (i.e. when {@link #depth()} is 0), otherwise {@code false}. This method
     * will only return {@code true} for the start node and only one time.
     * @return whether or not the traversal is at the start node or not.
     */
    boolean atStartNode();
}
