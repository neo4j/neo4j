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
        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    @Override
    public Map<String, Object> readMap( String input )
    {
        if ( empty( input ) )
        {
            return Collections.emptyMap();
        }
        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    @Override
    public List<Object> readList( String input )
    {
        if ( empty( input ) )
        {
            return Collections.emptyList();
        }
        throw new MediaTypeNotSupportedException( Response.Status.UNSUPPORTED_MEDIA_TYPE, supported, requested );
    }

    private boolean empty( String input )
    {
        return input == null || "".equals( input.trim() );
    }

    @Override
    protected String serializeValue( final String type, final Object value )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected ListWriter serializeList( final String type )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected MappingWriter serializeMapping( final String type )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected String complete( final ListWriter serializer )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }

    @Override
    protected String complete( final MappingWriter serializer )
    {
        throw new MediaTypeNotSupportedException( Response.Status.NOT_ACCEPTABLE, supported, requested );
    }
}
