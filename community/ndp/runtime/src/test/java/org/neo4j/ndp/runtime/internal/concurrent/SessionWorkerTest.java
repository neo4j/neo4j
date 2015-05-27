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
package org.neo4j.ndp.runtime.internal.concurrent;

import org.junit.Test;

import org.neo4j.function.Consumer;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.ndp.runtime.Session;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SessionWorkerTest
{
    @Test
    public void shouldExecuteWorkWhenRun() throws Throwable
    {
        // Given
        Session session = mock( Session.class );
        SessionWorker worker = new SessionWorker( session, NullLogService.getInstance() );
        worker.handle( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.run( "Hello, world!", null, null, null );
            }
        });
        worker.handle( SessionWorker.SHUTDOWN );

        // When
        worker.run();

        // Then
        verify( session ).run( "Hello, world!", null, null, null );
        verify( session ).close();
        verifyNoMoreInteractions( session );
    }

    @Test
    public void errorThrownDuringExecutionShouldCauseSessionClose() throws Throwable
    {
        // Given
        Session session = mock( Session.class );
        SessionWorker worker = new SessionWorker( session, NullLogService.getInstance() );
        worker.handle( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                throw new RuntimeException( "It didn't work out." );
            }
        });

        // When
        worker.run();

        // Then
        verify( session ).close();
    }
}