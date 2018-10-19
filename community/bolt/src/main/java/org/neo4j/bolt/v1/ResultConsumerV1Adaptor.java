package org.neo4j.bolt.v1;

import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.v4.ResultConsumer;

public class ResultConsumerV1Adaptor implements ResultConsumer
{
    private final boolean pull;
    private final StateMachineContext context;

    public ResultConsumerV1Adaptor( StateMachineContext context, boolean pull )
    {
        this.pull = pull;
        this.context = context;
    }

    @Override
    public boolean hasMore()
    {
        return false;
    }

    @Override
    public void consume( BoltResult boltResult ) throws Exception
    {
        context.connectionState().getResponseHandler().onRecords( boltResult, pull );
    }
}
