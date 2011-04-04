package org.neo4j.com;

import org.jboss.netty.channel.Channel;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.MadeUpClient.DumbRequestType;

public class MadeUpServer extends Server<MadeUpCommunicationInterface, Void>
{
    private boolean responseWritten;

    public MadeUpServer( MadeUpCommunicationInterface realMaster, int port )
    {
        super( realMaster, port, null );
    }

    @Override
    protected void responseWritten( RequestType<MadeUpCommunicationInterface> type, Channel channel,
            SlaveContext context )
    {
        responseWritten = true;
    }

    @Override
    protected RequestType<MadeUpCommunicationInterface> getRequestContext( byte id )
    {
        return DumbRequestType.values()[id];
    }

    @Override
    protected void finishOffConnection( Channel channel, SlaveContext context )
    {
    }
    
    public boolean responseHasBeenWritten()
    {
        return responseWritten;
    }
}
