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
package org.neo4j.collection.primitive;

public interface PrimitiveIntObjectVisitor<VALUE, E extends Exception>
{
    /**
     * Visit the given entry.
     *
     * @param key The key of the entry.
     * @param value The value of the entry.
     * @return 'true' to signal that the iteration should be stopped, 'false' to signal that the iteration should
     * continue if there are more entries to look at.
     * @throws E any thrown exception of type 'E' will bubble up through the 'visit' method.
     */
    boolean visited( int key, VALUE value ) throws E;
}
