/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.tx;

import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * Receives and unpacks {@link Response responses}.
 * Transaction obligations are handled by {@link TransactionObligationFulfiller} and
 * {@link TransactionStream transaction streams} are {@link TransactionCommitProcess committed to the store},
 * in batches.
 * <p/>
 * It is assumed that any {@link TransactionStreamResponse response carrying transaction data} comes from the one
 * and same thread.
 */
public class TransactionApplier
{
    private final TransactionRepresentationCommitProcess commitProcess;
    private final VersionContextSupplier versionContextSupplier;

    public TransactionApplier( DependencyResolver resolver )
    {
        commitProcess = new TransactionRepresentationCommitProcess(
                resolver.resolveDependency( TransactionAppender.class ),
                resolver.resolveDependency( StorageEngine.class ) );
        versionContextSupplier = resolver.resolveDependency( VersionContextSupplier.class );
    }

    public void appendToLogAndApplyToStore( CommittedTransactionRepresentation tx ) throws TransactionFailureException
    {
        commitProcess.commit( new TransactionToApply( tx.getTransactionRepresentation(),
                tx.getCommitEntry().getTxId(), versionContextSupplier.getVersionContext() ), NULL, EXTERNAL );
    }
}
