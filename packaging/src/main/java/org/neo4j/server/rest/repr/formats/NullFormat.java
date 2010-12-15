package org.neo4j.server.rest.repr.formats;

import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.MediaTypeNotSupportedException;
import org.neo4j.server.rest.repr.RepresentationFormat;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NullFormat extends RepresentationFormat
{
    private final Collection<MediaType> supported;
    private final MediaType[] requested;

    public NullFormat(Collection<MediaType> supported, MediaType... requested)
    {
        super( null );
        this.supported = supported;
        this.requested = requested;
    }

    @Override
    public Object readValue( String input )
    {
        if ( empty( input ) )
        {
            return null;
        }

        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    @Override
    public URI readUri( String input )
    {
        if ( empty( input ) )
        {
            return null;
        }
        throw new WebApplicationException( noInputType() );
    }

    @Override
    public Map<String, Object> readMap( String input )
    {
        if ( empty( input ) )
        {
            return Collections.emptyMap();
        }
        throw new WebApplicationException( noInputType() );
    }

    @Override
    public List<Object> readList( String input )
    {
        if ( empty( input ) )
        {
            return Collections.emptyList();
        }
        throw new WebApplicationException( noInputType() );
    }

    private boolean empty( String input )
    {
        return input == null || "".equals( input.trim() );
    }

    private Response noInputType()
    {
        return Response.status( Response.Status.UNSUPPORTED_MEDIA_TYPE ).entity(
                "Supplied data has no media type." ).build();
    }

    private Response noOutputType()
    {
        return Response.status( Response.Status.NOT_ACCEPTABLE ).entity(
                "Expected output type not supported." ).build();
    }

    @Override
    protected String serializeValue( final String type, final Object value )
    {
        throw new WebApplicationException( noOutputType() );
    }

    @Override
    protected ListWriter serializeList( final String type )
    {
        throw new WebApplicationException( noOutputType() );
    }

    @Override
    protected MappingWriter serializeMapping( final String type )
    {
        throw new WebApplicationException( noOutputType() );
    }

    @Override
    protected String complete( final ListWriter serializer )
    {
        throw new WebApplicationException( noOutputType() );
    }

    @Override
    protected String complete( final MappingWriter serializer )
    {
        throw new WebApplicationException( noOutputType() );
    }
}
