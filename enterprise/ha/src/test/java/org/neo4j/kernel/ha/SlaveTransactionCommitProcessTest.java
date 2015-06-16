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
package org.neo4j.kernel.ha;

import org.junit.Test;

import java.util.Collections;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlaveTransactionCommitProcessTest
{
    @Test
    public void shouldForwardLockIdentifierToMaster() throws Exception
    {
        // Given
        Master master = mock( Master.class );
        RequestContextFactory reqFactory = mock( RequestContextFactory.class );

        Response<Long> response = mock(Response.class);
        when(response.response()).thenReturn( 1l );

        when(master.commit( any( RequestContext.class), any( TransactionRepresentation.class) )).thenReturn( response );

        SlaveTransactionCommitProcess process = new SlaveTransactionCommitProcess( master, reqFactory );
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation(
                Collections.<Command>emptyList() );
        tx.setHeader(new byte[]{}, 1, 1, 1, 1, 1, 1337);

        // When
        process.commit(tx , new LockGroup(), CommitEvent.NULL, TransactionApplicationMode.INTERNAL );

        // Then
        verify( reqFactory ).newRequestContext( 1337 );
    }
}
