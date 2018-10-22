package org.neo4j.bolt.v4.messaging;

import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.StateMachineContext;

public class DiscardAllResultConsumer implements ResultConsumer
{
    private final StateMachineContext context;

    public DiscardAllResultConsumer( StateMachineContext context )
    {
        this.context = context;
    }

    @Override
    public void consume( BoltResult boltResult ) throws Exception
    {
        context.connectionState().getResponseHandler().onDiscardRecords( boltResult );
    }

    public boolean hasMore()
    {
        return false;
    }
}
