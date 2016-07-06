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
package org.neo4j.kernel.impl.transaction.log;

public class FakeCommitment implements Commitment
{
    public static final int CHECKSUM = 3;
    private final long id;
    private final TransactionIdStore transactionIdStore;
    private boolean committed;
    private boolean hasLegacyIndexChanges = false;

    public FakeCommitment( long id, TransactionIdStore transactionIdStore )
    {
        this( id, transactionIdStore, false );
    }

    public FakeCommitment( long id, TransactionIdStore transactionIdStore, boolean markedAsCommitted )
    {
        this.id = id;
        this.transactionIdStore = transactionIdStore;
        this.committed = markedAsCommitted;
    }

    @Override
    public void publishAsCommitted()
    {
        committed = true;
        transactionIdStore.transactionCommitted( id, CHECKSUM );
    }

    @Override
    public void publishAsClosed()
    {
        transactionIdStore.transactionClosed( id, 1, 2 );
    }

    @Override
    public boolean markedAsCommitted()
    {
        return committed;
    }

    public void setHasLegacyIndexChanges( boolean hasLegacyIndexChanges )
    {
        this.hasLegacyIndexChanges = hasLegacyIndexChanges;
    }

    @Override
    public boolean hasLegacyIndexChanges()
    {
        return hasLegacyIndexChanges;
    }
}
