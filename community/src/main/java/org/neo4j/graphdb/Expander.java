package org.neo4j.graphdb;

import org.neo4j.helpers.Predicate;

public interface Expander extends RelationshipExpander
{
    // Expansion<Relationship> expand( Node start );

    Expander reversed();

    Expander add( RelationshipType type );

    Expander add( RelationshipType type, Direction direction );

    Expander remove( RelationshipType type );

    Expander addNodeFilter( Predicate<? super Node> filter );

    Expander addRelationsipFilter( Predicate<? super Relationship> filter );
}
