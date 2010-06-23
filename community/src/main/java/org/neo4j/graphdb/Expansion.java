package org.neo4j.graphdb;

import org.neo4j.commons.Pair;
import org.neo4j.commons.Predicate;

public interface Expansion<T> extends Iterable<T>
{
    Expander expander();

    Expansion<Node> nodes();

    Expansion<Relationship> relationships();

    Expansion<Pair<Relationship, Node>> pairs();

    Expansion<T> including( RelationshipType type );

    Expansion<T> including( RelationshipType type, Direction direction );

    Expansion<T> excluding( RelationshipType type );

    Expansion<T> filterNodes( Predicate<? super Node> filter );

    Expansion<T> filterRelationships( Predicate<? super Relationship> filter );

    boolean isEmpty();

    T getSingle();
}
