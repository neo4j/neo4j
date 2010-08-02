package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;

/**
 * Represents a {@link Path position} and a {@link RelationshipExpander} with a
 * traversal context, for example parent and an iterator of relationships to go
 * next. It's a base to write a {@link BranchSelector} on top of.
 */
public interface TraversalBranch
{
    /**
     * The parent expansion source which created this {@link TraversalBranch}.
     * @return the parent of this expansion source.
     */
    TraversalBranch parent();

    /**
     * The position represented by this expansion source.
     * @return the position represented by this expansion source.
     */
    Path position();

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
    TraversalBranch next();

    /**
     * Returns the number of relationships this expansion source has expanded.
     * In this count isn't included the relationship which led to coming here
     * (since that could also be traversed, although skipped, when expanding
     * this source).
     *
     * @return the number of relationships this expansion source has expanded.
     */
    int expanded();
}
