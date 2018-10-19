package org.neo4j.bolt.v4;

import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.StateMachineContext;

public class PullResultConsumer implements ResultConsumer
{

    private final StateMachineContext context;
    private final long size;
    private boolean hasMore;

    public PullResultConsumer( StateMachineContext context, long size )
    {
        this.context = context;
        this.size = size;
    }

    @Override
    public void consume( BoltResult boltResult ) throws Exception
    {
        hasMore = context.connectionState().getResponseHandler().onPullRecords( boltResult, size );
    }

    public boolean hasMore()
    {
        return hasMore;
    }
}
