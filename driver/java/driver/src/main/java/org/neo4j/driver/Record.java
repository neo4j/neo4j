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
 * A record is a collection of named fields, and is what makes up the individual items in a {@link
 * org.neo4j.driver.Result}
 */
public interface Record
{
    /**
     * From the current record the result is pointing to, retrieve the value in the specified field.
     *
     * @param fieldIndex the field index into the current record
     * @return the value in the specified field
     */
    Value get( int fieldIndex );

    /**
     * From the current record the result is pointing to, retrieve the value in the specified field.
     *
     * @param fieldName the field field to retrieve the value from
     * @return the value in the specified field
     */
    Value get( String fieldName );

    /**
     * Get an ordered sequence of the field names in this result.
     *
     * @return field names
     */
    Iterable<String> fieldNames();

}
