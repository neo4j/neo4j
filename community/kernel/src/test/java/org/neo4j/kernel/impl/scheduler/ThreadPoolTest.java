package org.neo4j.kernel.impl.scheduler;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import org.neo4j.scheduler.Group;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ThreadPoolTest
{
    private ThreadPool threadPool;

    @BeforeEach
    void setup()
    {
        threadPool = new ThreadPool( Group.TESTING, new ThreadGroup( "TestPool" ), new ThreadPool.ThreadPoolParameters() );
    }

    @AfterEach
    public void teardown()
    {
        if ( threadPool != null )
        {
            threadPool.shutDown();
        }
    }

    @Test
    void poolDoesNotLeakFastJobs() throws ExecutionException, InterruptedException
    {
        // When
        var fastJob = threadPool.submit( () -> { /* do nothing */ } );
        fastJob.waitTermination();

        // Then
        assertEquals( 0, threadPool.activeJobCount(), "Active job count should be 0 when job is terminated" );
    }
}
