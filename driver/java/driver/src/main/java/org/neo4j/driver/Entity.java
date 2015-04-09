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
 * A uniquely identifiable property container that can form part of a Neo4j graph.
 */
public interface Entity
{
    /**
     * A unique {@link org.neo4j.driver.Identity identity} for this Entity. Identities are guaranteed
     * to remain stable for the duration of the session they were found in, but may be re-used for other
     * entities after that. As such, if you want a public identity to use for your entities, attaching
     * an explicit 'id' property or similar persistent and unique identifier is a better choice.
     *
     * @return an identity object
     */
    Identity identity();

    /**
     * Return all property keys.
     *
     * @return a property key Collection
     */
    Iterable<String> propertyKeys();

    /**
     * Return a specific property {@link org.neo4j.driver.Value}.
     *
     * @param key a property key
     * @return the property value
     */
    Value property( String key );
}