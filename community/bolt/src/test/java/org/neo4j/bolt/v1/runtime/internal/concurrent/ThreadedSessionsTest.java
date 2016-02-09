package org.neo4j.bolt.v1.runtime.internal.concurrent;


import org.junit.Test;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.Sessions;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.monitoring.Monitors;

import static org.mockito.Mockito.mock;

public class ThreadedSessionsTest
{
    @Test
    public void shouldMonitorSessions() throws Throwable
    {
        // Given
        Monitors monitors = new Monitors();
        ThreadedSessions sessions = new ThreadedSessions( mock( Sessions.class ), mock( JobScheduler.class ),
                NullLogService.getInstance() );

        Session session = sessions.newSession();

        // When
        session.run( "hello", null, null, null );

        // Then

    }
}