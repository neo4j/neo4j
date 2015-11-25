/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.catchup.tx.edge;

import java.io.IOException;

import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;

import static org.neo4j.kernel.impl.api.TransactionApplicationMode.EXTERNAL;

/**
 * Receives and unpacks {@link Response responses}.
 * Transaction obligations are handled by {@link TransactionObligationFulfiller} and
 * {@link TransactionStream transaction streams} are {@link TransactionRepresentationStoreApplier applied to the
 * store},
 * in batches.
 * <p/>
 * It is assumed that any {@link TransactionStreamResponse response carrying transaction data} comes from the one
 * and same thread.
 */
public class TransactionApplier
{
    private final TransactionAppender appender;
    private final TransactionRepresentationStoreApplier storeApplier;
    private final IndexUpdatesValidator indexUpdatesValidator;
    private final LogFile logFile;
    private final LogRotation logRotation;
    private final KernelHealth kernelHealth;

    public TransactionApplier( DependencyResolver resolver )
    {
        this.appender = resolver.resolveDependency( TransactionAppender.class );
        this.storeApplier = resolver.resolveDependency( TransactionRepresentationStoreApplier.class );
        this.indexUpdatesValidator = resolver.resolveDependency( IndexUpdatesValidator.class );
        this.logFile = resolver.resolveDependency( LogFile.class );
        this.logRotation = resolver.resolveDependency( LogRotation.class );
        this.kernelHealth = resolver.resolveDependency( KernelHealth.class );
    }

    public void appendToLogAndApplyToStore( CommittedTransactionRepresentation tx ) throws IOException
    {
        // Synchronize to guard for concurrent shutdown
        synchronized ( logFile )
        {
            // Check rotation explicitly, since the version of append that we're calling isn't doing that.
            logRotation.rotateLogIfNeeded( LogAppendEvent.NULL );

            try
            {
                LogEntryCommit commitEntry = tx.getCommitEntry();

                Commitment commitment = appender.append( tx.getTransactionRepresentation(), commitEntry.getTxId() );

                appender.force();

                long transactionId = commitEntry.getTxId();
                TransactionRepresentation representation = tx.getTransactionRepresentation();
                try
                {
                    commitment.publishAsCommitted();
                    try ( LockGroup locks = new LockGroup();
                          ValidatedIndexUpdates indexUpdates = indexUpdatesValidator.validate( representation) )
                    {
                        storeApplier.apply( representation, indexUpdates, locks, transactionId, EXTERNAL );
                    }
                }
                finally
                {
                    commitment.publishAsApplied();
                }
            }
            catch ( IOException e )
            {
                // Kernel panic is done on this level, i.e. append and apply doesn't do that themselves.
                kernelHealth.panic( e );
                throw e;
            }
        }
    }
}
