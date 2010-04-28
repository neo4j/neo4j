package org.neo4j.graphdb;

/**
 * An expander of relationships. It's a more flexible way of getting
 * relationships from a node.
 */
public interface RelationshipExpander
{
    /**
     * Returns relationships for a node in whatever way the implementation
     * likes.
     * 
     * @param node the node to expand.
     * @return the relationships to return for the {@code node}.
     */
    Iterable<Relationship> expand( Node node );

    /**
     * Returns relationships for a node in whatever way the implementation
     * likes. The direction (in which relationships are found) can be reversed
     * by setting {@code reversedDirection} to true.
     * 
     * @param node the node to expand.
     * @param reversedDirection whether or not to use reversed directions
     * when finding relationships.
     * @return the relationships to return for the {@code node}.
     */
    Iterable<Relationship> expand( Node node,
            boolean reversedDirection );
}
