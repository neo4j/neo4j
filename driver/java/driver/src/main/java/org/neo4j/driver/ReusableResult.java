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
 * A {@link Result} that has been fully retrieved and stored from the server.
 * It can therefore be kept outside the scope of the current transaction, iterated over multiple times and used while
 * other statements are issued.
 * For instance:
 * <p>
 * {@code
 * for(Record v : session.run( ".." ).retain() )
 * {
 * session.run( ".." );
 * }
 * }
 */
public interface ReusableResult extends Iterable<Record>
{
    /**
     * The number of values in this result.
     *
     * @return the result record count
     */
    long size();

    /**
     * Retrieve a record from this result based on its position in the original result stream.
     *
     * @param index retrieve an item in the result by index
     * @return the requested record
     */
    Record get( long index );
}
