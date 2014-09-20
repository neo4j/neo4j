/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

/**
 * Generates transaction ids.
 */
public interface TxIdGenerator
{
    /**
     * Generates a transaction id to use for the committing transaction.
     * TODO ideally {@link TransactionRepresentation} shouldn't have to be passed in here, but in order to
     * comply with the current HA architecture where, in the slave case, the transaction gets sent over to
     * the master and transaction id generated there.
     *
     * @param identifier temporary transaction identifier.
     * @return transaction id to use to commit the next transaction for
     * this {@code dataSource}.
     */
    long generate( TransactionRepresentation transaction );
}
