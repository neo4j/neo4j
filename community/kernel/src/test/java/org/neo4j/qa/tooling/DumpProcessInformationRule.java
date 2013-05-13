package org.neo4j.qa.tooling;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;
import org.neo4j.helpers.Pair;

public class DumpProcessInformationRule extends ExternalResource implements Callable<Void>
{
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool( 2 );

    private final String containing;
    private final File baseDir;
    private final long duration;
    private final TimeUnit timeUnit;

    private volatile ScheduledFuture<?> thunk = null;

    public DumpProcessInformationRule( String containing, File baseDir, long duration, TimeUnit timeUnit )
    {
        super();

        this.containing = containing;
        this.baseDir = baseDir;
        this.duration = duration;
        this.timeUnit = timeUnit;
    }

    @Override
    protected synchronized void before() throws Throwable
    {
        if ( null == thunk )
        {
            super.before();
            thunk = executor.schedule( this, duration, timeUnit );
        }
        else
            throw new IllegalStateException( "process dumping thunk already started" );
    }

    @Override
    protected synchronized void after()
    {
        if ( null != thunk && !thunk.isDone() )
            thunk.cancel( true );
        thunk = null;
        super.after();
    }

    @Override
    public Void call() throws Exception
    {
        for ( Pair<Long, String> pair : DumpProcessInformation.getJPids( containing ) )
        {
            DumpProcessInformation.doThreadDump( pair, baseDir );
        }
        return null;
    }
}
