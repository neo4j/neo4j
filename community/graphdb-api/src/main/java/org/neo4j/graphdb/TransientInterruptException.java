/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
 * The transaction processing thread was interrupted.
 * <p>
 * The threads interrupt flag has been lowered, but it is unspecified if the operation can be retried.
 * It is also unspecified if the transaction has been terminated or not, and whether or not a <em>kernel panic</em> has been raised.
 * <p>
 * Thread interrupts should be avoided, as it may inconvenience the database while doing IO.
 * Use {@link Transaction#terminate()} instead, if you wish to asynchronously stop a transaction.
 */
public class TransientInterruptException extends TransientFailureException
{
    public TransientInterruptException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public TransientInterruptException( String message )
    {
        super( message );
    }
}
