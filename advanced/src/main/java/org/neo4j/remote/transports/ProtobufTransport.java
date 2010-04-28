package org.neo4j.remote.transports;

import java.net.URI;

import org.neo4j.remote.ConnectionTarget;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.Transport;

/*public*/final class ProtobufTransport extends Transport
{
    public ProtobufTransport()
    {
        super( "protobuf" );
    }

    @Override
    protected boolean handlesUri( URI resourceUri )
    {
        return "protobuf".equals( resourceUri.getScheme() );
    }

    @Override
    protected ConnectionTarget create( URI resourceUri )
    {
        String scheme = resourceUri.getScheme();
        if ( "protobuf".equals( scheme ) )
        {
            return protobuf( resourceUri.getSchemeSpecificPart() );
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported protocol scheme: "
                                                + scheme );
        }
    }

    private ConnectionTarget protobuf( String target )
    {
        return new ConnectionTarget()
        {
            public RemoteConnection connect( String username, String password )
            {
                // TODO Auto-generated method stub
                return connect();
            }

            public RemoteConnection connect()
            {
                // TODO Auto-generated method stub
                return new ProtobufConnection();
            }
        };
    }
}
