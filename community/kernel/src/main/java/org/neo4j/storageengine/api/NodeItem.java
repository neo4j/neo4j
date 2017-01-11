/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.storageengine.api;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.cursor.IntCursor;

/**
 * Represents a single node from a cursor.
 */
public interface NodeItem
        extends EntityItem
{
    /**
     * @return label cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    PrimitiveIntSet labels();

    /**
     * @return relationship cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<RelationshipItem> relationships( Direction direction, int... typeIds );

    /**
     * @return relationship cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<RelationshipItem> relationships( Direction direction );

    /**
     * @return relationship type cursor for relationships attached to this node.
     * @throws IllegalStateException if no current node is selected
     */
    IntCursor relationshipTypes();

    /**
     * Returns degree, e.g. number of relationships for this node.
     *
     * @param direction {@link Direction} filter when counting relationships, e.g. only
     * {@link Direction#OUTGOING outgoing} or {@link Direction#INCOMING incoming}.
     * @return degree of relationships in the given direction.
     */
    int degree( Direction direction );

    /**
     * Returns degree, e.g. number of relationships for this node.
     *
     * @param direction {@link Direction} filter on when counting relationships, e.g. only
     * {@link Direction#OUTGOING outgoing} or {@link Direction#INCOMING incoming}.
     * @param typeId relationship type id to filter when counting relationships.
     * @return degree of relationships in the given direction and relationship type.
     */
    int degree( Direction direction, int typeId );

    /**
     * @return whether or not this node has been marked as being dense, i.e. exceeding a certain threshold
     * of number of relationships.
     */
    boolean isDense();

    /**
     * @return {@link Cursor} over all {@link DegreeItem}, i.e. all combinations of {@link Direction} and
     * relationship type ids for this node.
     */
    Cursor<DegreeItem> degrees();

    /**
     * @param labelId label token id to check.
     * @return whether or not this node has the given label.
     */
    boolean hasLabel( int labelId );
}
