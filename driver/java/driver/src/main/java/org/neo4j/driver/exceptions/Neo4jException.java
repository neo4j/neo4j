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
package org.neo4j.driver.exceptions;

/**
 * This is the base class for all Neo4j exceptions.
 */
public abstract class Neo4jException extends RuntimeException
{
    private final String code;

    public Neo4jException( String message )
    {
        this( "N/A", message );
    }

    public Neo4jException( String message, Throwable cause )
    {
        this( "N/A", message, cause );
    }

    public Neo4jException( String code, String message )
    {
        this( code, message, null );
    }

    public Neo4jException( String code, String message, Throwable cause )
    {
        super( message, cause );
        this.code = code;
    }

    /**
     * Access the standard Neo4j Status Code for this exception, you can use this to refer to the Neo4j manual for
     * details on what caused the error.
     *
     * @return the Neo4j Status Code for this exception, or 'N/A' if none is available
     */
    public String neo4jErrorCode()
    {
        return code;
    }
}
