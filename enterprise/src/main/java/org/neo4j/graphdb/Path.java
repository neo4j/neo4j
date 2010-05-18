package org.neo4j.graphdb;

/**
 * Represents a path in the graph. A path starts with a node followed by
 * pairs of {@link Relationship} and {@link Node} objects. The shortest path
 * is of length 0. Such a path contains only one node and no relationships.
 */
public interface Path
{
    /**
     * Returns the start node of this path. It's also the first node returned
     * from the {@link #nodes()} iterable.
     * @return the start node.
     */
    Node getStartNode();

    /**
     * Returns the end node of this path. It's also the last node returned
     * from {@link #nodes()} iterable. If the {@link #length()} of this path
     * is 0 the end node returned by this method is the same as the start node.
     * @return the end node.
     */
    Node getEndNode();

    /**
     * Returns all the relationships in between the nodes which this path
     * consists of.
     * @return the relationships in this path.
     */
    Iterable<Relationship> relationships();

    /**
     * Returns all the nodes in this path. The first node is the same as
     * {@link #getStartNode()} and the last node is the same as
     * {@link #getEndNode()}. In between those nodes can be an arbitrary
     * number of nodes. The shortest path possible is just one node,
     * where also the the start node is the same as the end node.
     * @return the nodes in this path.
     */
    Iterable<Node> nodes();

    /**
     * Returns the length of this path, i.e. the number of relationships
     * (which is the same as the number of nodes minus one). The shortest path
     * possible is of length 0.
     * 
     * @return the length (i.e. the number of relationships) in the path.
     */
    int length();

    /**
     * Returns a natural string representation of this path.
     * 
     * The string representation shows the nodes with relationships
     * (and their directions) in between them.
     * 
     * @return A string representation of the path.
     */
    @Override
    String toString();
}
