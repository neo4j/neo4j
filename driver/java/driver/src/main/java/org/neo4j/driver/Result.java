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
 * The result of running a statement, a stream of records. The result interface can be used to iterate over all the
 * records in the stream, and for each record to access the fields within it using the {@link #get(int) get} methods.
 * <p>
 * Results are valid until the next statement is run or until the end of the current transaction, whichever comes
 * first.
 * <p>
 * To keep a result around while further statements are run, or to use a result outside the scope of the current
 * transaction, see {@link #retain()}.
 */
public interface Result
{
    /**
     * Retrieve and store the entire result stream. This can be used if you want to
     * iterate over the stream multiple times or to store the whole result for later use.
     * <p>
     * This cannot be used if you have already started iterating through the stream using {@link #next()}.
     *
     * @return {@link org.neo4j.driver.ReusableResult}
     */
    ReusableResult retain();

    /**
     * Move to the next record in the result.
     *
     * @return true if there was another record, false if the stream is exhausted.
     */
    boolean next();

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

    /**
     * Retrieve the first field of the next record in the stream, and close the stream.
     * <p>
     * This is a utility for the common case of statements that are expected to yield a single output value.
     * <p>
     * <pre>
     * {@code
     * Record record = statement.run( "MATCH (n:User {uid:..}) RETURN n" ).single();
     * }
     * </pre>
     *
     * @return a single record from the stream
     * @throws org.neo4j.driver.exceptions.ClientException if the stream is empty
     */
    Record single();
}
