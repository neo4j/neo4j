package org.neo4j.bolt.v1.messaging;

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;

public abstract class BoltResponseHandlerV1Adaptor implements BoltResponseHandler
{
    public void onRecords( BoltResult result, boolean pullAll ) throws Exception
    {
        if ( pullAll )
        {
            boolean hasMore = onPullRecords( result, PullAllMessage.PULL_N_SIZE );
            if ( hasMore )
            {
                throw new IllegalArgumentException( "Shall not allow pulling records multiple times in bolt v1" );
            }
        }
        else
        {
            onDiscardRecords( result );
        }
    }
}
