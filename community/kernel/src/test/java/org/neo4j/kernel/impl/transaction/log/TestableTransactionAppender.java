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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;

public class TestableTransactionAppender implements TransactionAppender
{
    private final TransactionIdStore transactionIdStore;

    public TestableTransactionAppender( TransactionIdStore transactionIdStore )
    {
        this.transactionIdStore = transactionIdStore;
    }

    @Override
    public long append( TransactionToApply batch, LogAppendEvent logAppendEvent )
    {
        long txId = TransactionIdStore.BASE_TX_ID;
        while ( batch != null )
        {
            txId = transactionIdStore.nextCommittingTransactionId();
            batch.commitment( new FakeCommitment( txId, transactionIdStore ), txId );
            batch.commitment().publishAsCommitted();
            batch = batch.next();
        }
        return txId;
    }

    @Override
    public void checkPoint( LogPosition logPosition, LogCheckPointEvent logCheckPointEvent )
    {
    }
}
