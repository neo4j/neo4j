package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;

/**
 * Represents a {@link Position} and a {@link RelationshipExpander} with a
 * traversal context, f.ex. parent and an iterator of relationships to go next.
 * It's a base to write a {@link SourceSelector} on top of.
 */
public interface ExpansionSource
{
    /**
     * The parent expansion source which created this {@link ExpansionSource}.
     * @return the parent of this expansion source.
     */
    ExpansionSource parent();
    
    /**
     * The position represented by this expansion source.
     * @return the position represented by this expansion source.
     */
    Position position();
    
    /**
     * The depth for this expansion source compared to the start node of the
     * traversal.
     * @return the depth of this expansion source.
     */
    int depth();
    
    /**
     * The node for this expansion source.
     * @return the node for this expansion source.
     */
    Node node();
    
    /**
     * The relationship for this expansion source. It's the relationship
     * which was traversed to get to this expansion source.
     * @return the relationship for this expansion source.
     */
    Relationship relationship();
    
    /**
     * Returns the next expansion source from the expanded relationships
     * from the current node.
     * 
     * @return the next expansion source from this expansion source.
     */
    ExpansionSource next();
}
