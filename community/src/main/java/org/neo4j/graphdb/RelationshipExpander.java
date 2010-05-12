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
     * Returns a new instances with the exact same {@link RelationshipType}s,
     * but with all directions reversed (Direction{@link #reversed()}.
     * 
     * @return a {@link RelationshipExpander} with the same types, but with
     * reversed directions.
     */
    RelationshipExpander reversed();
}
