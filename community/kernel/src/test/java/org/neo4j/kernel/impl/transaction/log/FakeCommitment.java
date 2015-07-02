/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

public class FakeCommitment implements Commitment
{
    private final long id;
    private final TransactionIdStore transactionIdStore;

    public FakeCommitment( long id, TransactionIdStore transactionIdStore )
    {
        this.id = id;
        this.transactionIdStore = transactionIdStore;
    }

    @Override
    public void publishAsCommitted()
    {
        transactionIdStore.transactionCommitted( id, 3 );
    }

    @Override
    public void publishAsApplied()
    {
        transactionIdStore.transactionClosed( id, 1, 2 );
    }

    @Override
    public long transactionId()
    {
        return id;
    }
}
