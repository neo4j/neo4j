/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;

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

    /**
     * @deprecated use {@link #filterNodes(Predicate)} instead
     */
    @Deprecated
    Expansion<T> filterNodes( org.neo4j.helpers.Predicate<? super Node> filter );

    Expansion<T> filterNodes( Predicate<? super Node> filter );

    /**
     * @deprecated use {@link #filterRelationships(Predicate)} instead
     */
    @Deprecated
    Expansion<T> filterRelationships( org.neo4j.helpers.Predicate<? super Relationship> filter );

    Expansion<T> filterRelationships( Predicate<? super Relationship> filter );

    boolean isEmpty();

    T getSingle();
}
