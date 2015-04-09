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
 * An item that can be considered to have <em>direction</em>.
 * This is represented by the presence of <strong>start</strong> and <strong>end</strong> attributes.
 *
 * @param <T> the type of the objects at the start and end of this directed item
 */
public interface Directed<T>
{
    /** @return the start item from this directed sequence */
    T start();

    /** @return the end item from this directed sequence */
    T end();
}
