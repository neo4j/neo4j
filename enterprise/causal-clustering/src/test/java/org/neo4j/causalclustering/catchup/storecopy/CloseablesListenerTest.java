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
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertTrue;

public class CloseablesListenerTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldCloseAllReourcesBeforeException() throws Exception
    {
        // given
        CloseablesListener closeablesListener = new CloseablesListener();
        RuntimeException exception = new RuntimeException( "fail" );
        CloseTrackingCloseable kindCloseable1 = new CloseTrackingCloseable();
        CloseTrackingCloseable unkindCloseable = new CloseTrackingCloseable( exception );
        CloseTrackingCloseable kindCloseable2 = new CloseTrackingCloseable();
        closeablesListener.add( kindCloseable1 );
        closeablesListener.add( unkindCloseable );
        closeablesListener.add( kindCloseable2 );

        //then we expect an exception
        expectedException.expect( exception.getClass() );

        // when
        closeablesListener.close();

        //then we expect all have closed
        assertTrue( kindCloseable1.wasClosed );
        assertTrue( unkindCloseable.wasClosed );
        assertTrue( kindCloseable2.wasClosed );
    }

    class CloseTrackingCloseable implements AutoCloseable
    {
        private final Exception throwOnClose;

        private CloseTrackingCloseable()
        {
            this( null );
        }

        CloseTrackingCloseable( Exception throwOnClose )
        {
            this.throwOnClose = throwOnClose;
        }

        boolean wasClosed;

        @Override
        public void close() throws Exception
        {
            wasClosed = true;
            if ( throwOnClose != null )
            {
                throw throwOnClose;
            }
        }
    }
}
