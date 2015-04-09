/**
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
package org.neo4j.driver.internal;

import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.driver.internal.spi.Connection;

import static java.util.Collections.EMPTY_MAP;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class StandardTransactionTest
{
    @Test
    public void shouldRollbackOnNoExplicitSuccess() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        Runnable cleanup = mock( Runnable.class );
        StandardTransaction tx = new StandardTransaction( conn, cleanup );

        // When
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).run( "ROLLBACK", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }

    @Test
    public void shouldRollbackOnExplicitFailure() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        Runnable cleanup = mock( Runnable.class );
        StandardTransaction tx = new StandardTransaction( conn, cleanup );

        tx.failure();
        tx.success(); // even if success is called after the failure call!

        // When
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).run( "ROLLBACK", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }

    @Test
    public void shouldCommitOnSuccess() throws Throwable
    {
        // Given
        Connection conn = mock( Connection.class );
        Runnable cleanup = mock( Runnable.class );
        StandardTransaction tx = new StandardTransaction( conn, cleanup );

        tx.success();

        // When
        tx.close();

        // Then
        InOrder order = inOrder( conn );
        order.verify( conn ).run( "BEGIN", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).run( "COMMIT", EMPTY_MAP, null );
        order.verify( conn ).discardAll();
        order.verify( conn ).sync();
        verify( cleanup ).run();
        verifyNoMoreInteractions( conn, cleanup );
    }
}