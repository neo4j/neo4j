/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
 * Signals that the transaction within which the failed operations ran
 * has been terminated with {@link Transaction#terminate()}.
 */
public class TransactionTerminatedException extends TransactionFailureException
{
    public TransactionTerminatedException()
    {
        super( "The transaction has been terminated, no new operations in it " +
               "are allowed. This normally happens because a client explicitly asks to terminate the transaction, " +
               "for instance to stop a long-running operation. It may also happen because an operator has asked the " +
               "database to be shut down, or because the current instance is about to perform a cluster role change. " +
               "Simply retry your operation in a new transaction, and you should see a successful result." );
    }
}
