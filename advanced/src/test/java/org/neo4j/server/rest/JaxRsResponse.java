package org.neo4j.server.rest;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.core.util.StringKeyObjectValueIgnoreCaseMultivaluedMap;
import com.sun.net.httpserver.Headers;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class JaxRsResponse extends Response
{

    private ClientResponse jettyResponse;

    public JaxRsResponse( ClientResponse jettyResponse )
    {
        this.jettyResponse = jettyResponse;
    }

    @Override
    public Object getEntity()
    {
        return jettyResponse.getEntity( Object.class );
    }

    @Override
    public int getStatus()
    {
        return jettyResponse.getStatus();
    }

    @Override
    public MultivaluedMap<String, Object> getMetadata()
    {
        MultivaluedMap<String, Object> metadata = new StringKeyObjectValueIgnoreCaseMultivaluedMap();
        for ( Map.Entry<String, List<String>> header : jettyResponse.getHeaders().entrySet() )
        {
            for ( Object value : header.getValue() )
            {
                metadata.putSingle( header.getKey(), value );
            }
        }
        return metadata;
    }

    public Map<String, List<String>> getHeaders()
    {
        Headers headers = new Headers();
        headers.putAll( jettyResponse.getHeaders() );
        return headers;
    }

    public <T> T getEntity( Class<T> asType )
    {
        return jettyResponse.getEntity(asType);
    }

    public URI getLocation() throws URISyntaxException
    {
        return new URI(getHeaders().get( HttpHeaders.LOCATION).get(0));
    }
}
