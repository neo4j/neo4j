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
package org.neo4j.graphdb;

/**
 * This exception is thrown from the {@link GraphDatabaseService#execute(String, java.util.Map) execute method}
 * when there is an error during the execution of a query.
 */
public class QueryExecutionException extends RuntimeException
{
    private final String statusCode;

    public QueryExecutionException( String message, Throwable cause, String statusCode )
    {
        super( message, cause );
        this.statusCode = statusCode;
    }

    /**
     * The Neo4j error <a href="http://neo4j.com/docs/stable/status-codes.html">status code</a>.
     *
     * @return the Neo4j error status code.
     */
    public String getStatusCode()
    {
        return statusCode;
    }
}
