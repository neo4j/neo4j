/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver;

/**
 * A unique identifier for an {@link org.neo4j.driver.Entity}.
 * <p>
 * The identity can be used to correlate entities in one response with entities received earlier. The identity of an
 * entity is guaranteed to be stable within the scope of a session. Beyond that, the identity may change. If you want
 * stable 'ids' for your entities, you may choose to add an 'id' property with a {@link java.util.UUID} or similar
 * unique value.
 */
public interface Identity
{
    // Force implementation
    @Override
    boolean equals( Object other );

    // Force implementation
    @Override
    int hashCode();
}
