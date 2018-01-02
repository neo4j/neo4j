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
package org.neo4j.kernel.impl.transaction.log.stresstest.workload;

import java.io.IOException;

import org.neo4j.function.BooleanSupplier;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;

class Worker implements Runnable
{
    private final TransactionAppender transactionAppender;
    private final TransactionRepresentationFactory factory;
    private final BooleanSupplier condition;

    public Worker( TransactionAppender transactionAppender,
            TransactionRepresentationFactory factory, BooleanSupplier condition )
    {
        this.transactionAppender = transactionAppender;
        this.factory = factory;
        this.condition = condition;
    }

    @Override
    public void run()
    {
        long latestTxId = TransactionIdStore.BASE_TX_ID;
        while ( condition.getAsBoolean() )
        {
            TransactionRepresentation representation = factory.nextTransaction( latestTxId );
            try
            {
                Commitment commitment = transactionAppender.append( representation, LogAppendEvent.NULL );
                commitment.publishAsCommitted();
                latestTxId = commitment.transactionId();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
