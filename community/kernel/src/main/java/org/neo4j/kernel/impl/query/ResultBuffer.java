/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.query;

import org.neo4j.values.AnyValue;

/**
 * Result buffer holding the results of a query execution.
 */
public interface ResultBuffer
{
    /**
     * Signal that a new query execution is starting, which will return {@code valuesPerResult}  many values per result.
     */
    void beginQueryExecution( int valuesPerResult );

    /**
     * Prepare the result stage for the next result row.
     *
     * @return the id of the new row, or -1 if the stage could not be cleared.
     */
    long beginResultRow() throws ResultBufferException;

    /**
     * Write a value of the result stage.
     *
     * This method is expected to be called for every valueId in the range [0..valuesPerResult), for every row.
     *
     * @param columnId column id of the value to write.
     * @param value the Value to write.
     */
    void writeValue( int columnId, AnyValue value );

    /**
     * Commit the result row in the result stage to the buffer.
     *
     * @return true if the buffer can accept the next result row.
     */
    boolean endResultRow() throws ResultBufferException;

// Suggestion for additional methods: add if needed.

//    /**
//     * Signals to the buffer that some error occurred during query execution.
//     *
//     * After onError is called, only endQueryExecution() will be called by this query.
//     *
//     * @param status The status code of the error
//     * @param msg The error message associated with the error, or null if no message is provided
//     */
//    void onError( Status status, String msg );
//
//    /**
//     * Signal that the current query execution is completed.
//     */
//    void endQueryExecution();
}
