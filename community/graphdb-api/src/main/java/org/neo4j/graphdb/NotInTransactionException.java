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
package org.neo4j.graphdb;

/**
 * Thrown when attempting to access or modify the graph outside of a transaction.
 *
 * @see Transaction
 */
public class NotInTransactionException extends RuntimeException
{
    public NotInTransactionException()
    {
        super( "The requested operation cannot be performed, because it has to be performed in a transaction. " +
                "Ensure you are wrapping your operation in the appropriate transaction boilerplate and try again." );
    }

    public NotInTransactionException( String message )
    {
        super( message );
    }

    public NotInTransactionException( Throwable cause )
    {
        super( cause );
    }

    public NotInTransactionException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
