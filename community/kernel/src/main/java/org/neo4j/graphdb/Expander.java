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
package org.neo4j.graphdb;

import org.neo4j.function.Predicate;

/**
 * This interface is an extension of the {@link RelationshipExpander} interface
 * that makes it possible to build customized versions of an {@link Expander}.
 *
 * @deprecated Deprecated because {@link RelationshipExpander} is deprecated. See {@link PathExpander} instead.
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
     * @param type      relationship type
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
     * @deprecated use {@link #addNodeFilter(Predicate)} instead
     */
    @Deprecated
    Expander addNodeFilter( org.neo4j.helpers.Predicate<? super Node> filter );

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
     * @deprecated because of typo, use {@link Expander#addRelationshipFilter(Predicate)} instead
     */
    @Deprecated
    Expander addRelationsipFilter( org.neo4j.helpers.Predicate<? super Relationship> filter );

    /**
     * Add a {@link Relationship} filter.
     *
     * @param filter filter to use
     * @return new instance
     * @deprecated because of typo, use {@link Expander#addRelationshipFilter(Predicate)} instead
     */
    Expander addRelationsipFilter( Predicate<? super Relationship> filter );

    /**
     * Add a {@link Relationship} filter.
     *
     * @param filter filter to use
     * @return new instance
     * @deprecated use {@link #addRelationshipFilter(Predicate)} instead
     */
    @Deprecated
    Expander addRelationshipFilter( org.neo4j.helpers.Predicate<? super Relationship> filter );

    /**
     * Add a {@link Relationship} filter.
     *
     * @param filter filter to use
     * @return new instance
     */
    Expander addRelationshipFilter( Predicate<? super Relationship> filter );
}
