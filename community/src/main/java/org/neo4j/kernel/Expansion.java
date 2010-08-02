package org.neo4j.kernel;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;

// Tentative Expansion API
interface Expansion<T> extends Iterable<T>
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
