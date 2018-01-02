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
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SimpleStatementLocksTest
{
    @Test
    public void shouldUseSameClientForImplicitAndExplicit() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // THEN
        assertSame( client, statementLocks.pessimistic() );
        assertSame( client, statementLocks.optimistic() );
    }

    @Test
    public void shouldDoNothingWithClientWhenPreparingForCommit() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // WHEN
        statementLocks.prepareForCommit();

        // THEN
        verifyNoMoreInteractions( client );
    }

    @Test
    public void shouldStopUnderlyingClient() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // WHEN
        statementLocks.stop();

        // THEN
        verify( client ).stop();
    }

    @Test
    public void shouldCloseUnderlyingClient() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // WHEN
        statementLocks.close();

        // THEN
        verify( client ).close();
    }
}
