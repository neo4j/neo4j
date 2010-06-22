package org.neo4j.graphdb;

import org.neo4j.commons.Predicate;

public interface Expansion<T extends PropertyContainer> extends Iterable<T>
{
    Expander expander();

    Expansion<Node> nodes();

    Expansion<Relationship> relationships();

    Expansion<T> add( RelationshipType type );

    Expansion<T> add( RelationshipType type, Direction direction );

    Expansion<T> exclude( RelationshipType type );

    Expansion<T> filterNodes( Predicate<? super Node> filter );

    Expansion<T> filterRelationships( Predicate<? super Relationship> filter );

    boolean isEmpty();

    T getSingle();
}
