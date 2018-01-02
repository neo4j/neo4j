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
package org.neo4j.server.rest.transactional;

import org.neo4j.server.rest.transactional.error.TransactionLifecycleException;

/**
 * Stores transaction contexts for the server, including handling concurrency safe ways to acquire
 * transaction contexts back, as well as timing out and closing transaction contexts that have been
 * left unused.
 */
public interface TransactionRegistry
{
    long begin( TransactionHandle handle );

    long release( long id, TransactionHandle transactionHandle );

    TransactionHandle acquire( long id ) throws TransactionLifecycleException;

    void forget( long id );

    TransactionHandle terminate( long id ) throws TransactionLifecycleException;

    void rollbackAllSuspendedTransactions();
}
