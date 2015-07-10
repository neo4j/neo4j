/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.cursor;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;

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
    Cursor<LabelItem> labels();

    /**
     * @param labelId for specific label to find
     * @return label cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<LabelItem> label( int labelId );

    /**
     * @return relationship cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<RelationshipItem> relationships( Direction direction, int... relTypes );

    /**
     * @return relationship cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<RelationshipItem> relationships( Direction direction );
}
