/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedTransactionCommitProcessTest
{
    private Replicator replicator = mock( Replicator.class );
    private TransactionRepresentation tx = mock( TransactionRepresentation.class );

    @Before
    public void tx()
    {
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
    }

    @Test
    public void shouldReplicateTransaction() throws Exception
    {
        // given
        CompletableFuture<Object> futureTxId = new CompletableFuture<>();
        futureTxId.complete( 5L );

        when( replicator.replicate( any( ReplicatedContent.class ), anyBoolean() ) ).thenReturn( futureTxId );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator );

        // when
        long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );

        // then
        assertEquals( 5, txId );
    }
}
