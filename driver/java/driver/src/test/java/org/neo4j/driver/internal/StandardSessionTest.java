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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.spi.Connection;

import static junit.framework.TestCase.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class StandardSessionTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldSyncOnRun() throws Throwable
    {
        // Given
        Connection mock = mock( Connection.class );
        StandardSession sess = new StandardSession( mock );

        // When
        sess.run( "whatever" );

        // Then
        verify( mock ).sync();
    }

    @Test
    public void shouldNotAllowNewTxWhileOneIsRunning() throws Throwable
    {
        // Given
        Connection mock = mock( Connection.class );
        StandardSession sess = new StandardSession( mock );
        sess.newTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        sess.newTransaction();
    }

    @Test
    public void shouldBeAbleToOpenTxAfterPreviousIsClosed() throws Throwable
    {
        // Given
        Connection mock = mock( Connection.class );
        StandardSession sess = new StandardSession( mock );
        sess.newTransaction().close();

        // When
        Transaction tx = sess.newTransaction();

        // Then we should've gotten a transaction object back
        assertNotNull( tx );
    }
}