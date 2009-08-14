/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.api.core;

/**
 * An exception that is thrown whenever a Neo API operation that requires a 
 * transaction is executed but no transaction is running.
 * <p>
 * Note, currently this exception is not guaranteed to be thrown. A read only 
 * operation may succeed if all the data is already cached. A modifying 
 * operation will however always throw this exception if no transaction is 
 * running.
 * 
 * @see Transaction
 */
public class NotInTransactionException extends RuntimeException
{
    public NotInTransactionException()
    {
        super();
    }

    public NotInTransactionException( String message )
    {
        super( message );
    }

    public NotInTransactionException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public NotInTransactionException( Throwable cause )
    {
        super( cause );
    }
}