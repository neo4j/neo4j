package org.neo4j.bolt.v4;

import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.function.ThrowingConsumer;

public class ResultConsumer implements ThrowingConsumer<BoltResult, Exception>
{

    final StateMachineContext context;
    final long size;
    boolean hasMore;

    public ResultConsumer( StateMachineContext context, long size )
    {
        this.context = context;
        this.size = size;
    }

    @Override
    public void accept( BoltResult boltResult ) throws Exception
    {
        hasMore = context.connectionState().getResponseHandler().onRecords( boltResult, size );
    }

    public boolean hasMore()
    {
        return hasMore;
    }
}
