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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.LongConsumer;

public class CheckPointTransactionCountThreshold implements CheckPointThreshold, LongConsumer
{
    private final AtomicInteger committedTransactions = new AtomicInteger( 0 );
    private final int threshold;

    public CheckPointTransactionCountThreshold( int threshold )
    {
        this.threshold = threshold;
    }

    @Override
    public boolean isCheckPointingNeeded()
    {
        return committedTransactions.get() >= threshold;
    }

    @Override
    public void checkPointHappened()
    {
        committedTransactions.set( 0 );
    }

    @Override
    public void accept( long transactionId )
    {
        committedTransactions.incrementAndGet();
    }
}
