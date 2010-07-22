package org.neo4j.graphdb;

import org.neo4j.helpers.Predicate;

/**
 * This interface is an extension of the {@link RelationshipExpander} interface
 * that makes it possible to build customized versions of an {@link Expander}.
 */
public interface Expander extends RelationshipExpander
{
    // Expansion<Relationship> expand( Node start );

    Expander reversed();

    /**
     * Add a {@link RelationshipType} to the {@link Expander}.
     * 
     * @param type relationship type
     * @return new instance
     */
    Expander add( RelationshipType type );

    /**
     * Add a {@link RelationshipType} with a {@link Direction} to the
     * {@link Expander}.
     * 
     * @param type relationship type
     * @param direction expanding direction
     * @return new instance
     */
    Expander add( RelationshipType type, Direction direction );

    /**
     * Remove a {@link RelationshipType} from the {@link Expander}.
     * 
     * @param type relationship type
     * @return new instance
     */
    Expander remove( RelationshipType type );

    /**
     * Add a {@link Node} filter.
     * 
     * @param filter filter to use
     * @return new instance
     */
    Expander addNodeFilter( Predicate<? super Node> filter );

    /**
     * Add a {@link Relationship} filter.
     * 
     * @param filter filter to use
     * @return new instance
     */
    Expander addRelationsipFilter( Predicate<? super Relationship> filter );
}
