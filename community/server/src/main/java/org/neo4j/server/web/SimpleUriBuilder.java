package org.neo4j.server.web;

import java.net.URI;
import java.net.URISyntaxException;

public class SimpleUriBuilder {

    public URI buildURI(String address, int port, boolean isSsl)
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "http" );
        
        if ( isSsl )
        {
            sb.append( "s" );

        }
        sb.append( "://" );

        sb.append( address );

        if ( port != 80 && port != 443)
        {
            sb.append( ":" );
            sb.append( port );
        }
        sb.append( "/" );

        try
        {
            return new URI( sb.toString() );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

}
