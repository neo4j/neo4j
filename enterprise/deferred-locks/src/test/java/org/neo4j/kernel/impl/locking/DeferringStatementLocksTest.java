/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import org.neo4j.storageengine.api.lock.ResourceType;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DeferringStatementLocksTest
{
    @Test
    public void shouldUseCorrectClientForImplicitAndExplicit()
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final DeferringStatementLocks statementLocks = new DeferringStatementLocks( client );

        // THEN
        assertSame( client, statementLocks.pessimistic() );
        assertThat( statementLocks.optimistic(), instanceOf( DeferringLockClient.class ) );
    }

    @Test
    public void shouldDoNothingWithClientWhenPreparingForCommitWithNoLocksAcquired()
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final DeferringStatementLocks statementLocks = new DeferringStatementLocks( client );

        // WHEN
        statementLocks.prepareForCommit( LockTracer.NONE );

        // THEN
        verify( client ).prepare();
        verifyNoMoreInteractions( client );
    }

    @Test
    public void shouldPrepareExplicitForCommitWhenLocksAcquire()
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final DeferringStatementLocks statementLocks = new DeferringStatementLocks( client );

        // WHEN
        statementLocks.optimistic().acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        statementLocks.optimistic().acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 42 );
        verify( client, never() ).acquireExclusive( eq( LockTracer.NONE ), any( ResourceType.class ), anyLong() );
        statementLocks.prepareForCommit( LockTracer.NONE );

        // THEN
        verify( client ).prepare();
        verify( client ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        verify( client ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 42 );
        verifyNoMoreInteractions( client );
    }

    @Test
    public void shouldStopUnderlyingClient()
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final DeferringStatementLocks statementLocks = new DeferringStatementLocks( client );

        // WHEN
        statementLocks.stop();

        // THEN
        verify( client ).stop();
    }

    @Test
    public void shouldCloseUnderlyingClient()
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final DeferringStatementLocks statementLocks = new DeferringStatementLocks( client );

        // WHEN
        statementLocks.close();

        // THEN
        verify( client ).close();
    }
}
