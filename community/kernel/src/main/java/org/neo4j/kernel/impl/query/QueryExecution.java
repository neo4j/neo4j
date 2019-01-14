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

/**
 * The execution of a query.
 */
public interface QueryExecution
{

    /**
     * The names of the result columns
     *
     * @return Array containing the names of the result columns in order.
     */
    String[] header();

    /**
     * Returns the result buffer of this execution.
     *
     * @return the result buffer of this execution.
     */
    ResultBuffer resultBuffer();

    /**
     * Wait for more results to be written to the resultBuffer.
     *
     * @return true if more results were written.
     */
    boolean waitForResult();

    /**
     * Terminate this execution, throwing away any buffered results, and releasing any other resources.
     *
     * TODO: what should this really be called... close? abort? terminate?
     */
    void terminate();
}
