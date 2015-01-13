/**
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

import java.io.IOException;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

public class PhysicalTransactionAppender extends AbstractPhysicalTransactionAppender
{
    public PhysicalTransactionAppender( LogFile logFile, LogRotation logRotation,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering, KernelHealth kernelHealth )
    {
        super( logFile, logRotation, transactionMetadataCache, transactionIdStore,
                legacyIndexTransactionOrdering, kernelHealth );
    }

    @Override
    protected void emptyBufferIntoChannel() throws IOException
    {
        channel.emptyBufferIntoChannelAndClearIt();
    }

    @Override
    protected void forceAfterAppend( LogAppendEvent logAppendEvent ) throws IOException
    {
        forceChannel();
    }
}
