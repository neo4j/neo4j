package org.neo4j.kernel.impl.query;

import java.util.List;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;

public class RecordingQuerySubscriber implements QuerySubscriber
{
    private List<AnyValue[]> all;
    private AnyValue[] current;
    private Throwable throwable;
    private QueryStatistics statistics;

    @Override
    public void onResult( int numberOfFields )
    {
        this.current = new AnyValue[numberOfFields];
    }

    @Override
    public void onRecord()
    {
        //do nothing
    }

    @Override
    public void onField( int offset, AnyValue value )
    {
        current[offset] = value;
    }

    @Override
    public void onRecordCompleted()
    {
        all.add( current.clone() );
    }

    @Override
    public void onError( Throwable throwable )
    {
        this.throwable = throwable;
    }

    @Override
    public void onResultCompleted( QueryStatistics statistics )
    {
        this.statistics = statistics;
    }

    public List<AnyValue[]> getOrThrow() throws Throwable
    {
        if (throwable != null )
        {
            throw throwable;
        }

        return all;
    }
}
