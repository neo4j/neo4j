/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal.concurrent;

import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.kernel.impl.logging.NullLogService;

public class SessionWorkerTest
{
    @Test
    public void shouldExecuteWorkWhenRun() throws Throwable
    {
        // Given
        Session session = Mockito.mock( Session.class );
        SessionWorker worker = new SessionWorker( session, NullLogService.getInstance() );
        worker.handle( s -> s.run( "Hello, world!", null, null, null ) );
        worker.handle( SessionWorker.SHUTDOWN );

        // When
        worker.run();

        // Then
        Mockito.verify( session ).run( "Hello, world!", null, null, null );
        Mockito.verify( session ).close();
        Mockito.verifyNoMoreInteractions( session );
    }

    @Test
    public void errorThrownDuringExecutionShouldCauseSessionClose() throws Throwable
    {
        // Given
        Session session = Mockito.mock( Session.class );
        SessionWorker worker = new SessionWorker( session, NullLogService.getInstance() );
        worker.handle( s -> {
            throw new RuntimeException( "It didn't work out." );
        } );

        // When
        worker.run();

        // Then
        Mockito.verify( session ).close();
    }
}
