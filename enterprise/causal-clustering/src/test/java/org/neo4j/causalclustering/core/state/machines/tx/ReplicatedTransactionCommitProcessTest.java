/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.tx;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings( "unchecked" )
public class ReplicatedTransactionCommitProcessTest
{
    private Replicator replicator = mock( Replicator.class );

    @Test
    public void shouldReplicateTransaction() throws Exception
    {
        // given
        CompletableFuture<Object> futureTxId = new CompletableFuture<>();
        futureTxId.complete( 5L );

        when( replicator.replicate( any( ReplicatedContent.class ), anyBoolean() ) ).thenReturn( futureTxId );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator );

        // when
        long txId = commitProcess.commit( tx(), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );

        // then
        assertEquals( 5, txId );
    }

    private TransactionToApply tx()
    {
        TransactionRepresentation tx = mock( TransactionRepresentation.class );
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
        return new TransactionToApply( tx );
    }
}
