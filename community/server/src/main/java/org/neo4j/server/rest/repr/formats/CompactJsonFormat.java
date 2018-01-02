/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr.formats;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.DefaultFormat;
import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationFormat;

import static org.neo4j.server.rest.domain.JsonHelper.assertSupportedPropertyValue;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;

@Service.Implementation( RepresentationFormat.class )
public class CompactJsonFormat extends RepresentationFormat
{
    public static final MediaType MEDIA_TYPE = new MediaType( MediaType.APPLICATION_JSON_TYPE.getType(),
            MediaType.APPLICATION_JSON_TYPE.getSubtype(), MapUtil.stringMap( "compact", "true" ) );

    public CompactJsonFormat()
    {
        super( MEDIA_TYPE );
    }

    private enum MappingTemplate
    {
        NODE( Representation.NODE )
        {
            @Override
            String render( Map<String, Object> serialized )
            {

                return JsonHelper.createJsonFrom( MapUtil.map( "self", serialized.get( "self" ), "data",
                        serialized.get( "data" ) ) );
            }
        },
        RELATIONSHIP( Representation.RELATIONSHIP )
        {
            @Override
            String render( Map<String, Object> serialized )
            {

                return JsonHelper.createJsonFrom( MapUtil.map( "self", serialized.get( "self" ), "data",
                        serialized.get( "data" ) ) );
            }
        },
        STRING( Representation.STRING )
        {
            @Override
            String render( Map<String, Object> serialized )
            {

                return JsonHelper.createJsonFrom( serialized );
            }
        },
        EXCEPTION( Representation.EXCEPTION )
        {

            @Override
            String render( Map<String, Object> data )
            {
                return JsonHelper.createJsonFrom( data );
            }

        };
        private final String key;

        private MappingTemplate( String key )
        {
            this.key = key;
        }

        static final Map<String, MappingTemplate> TEMPLATES = new HashMap<String, MappingTemplate>();
        static
        {
            for ( MappingTemplate template : values() )
                TEMPLATES.put( template.key, template );
        }

        abstract String render( Map<String, Object> data );
    }

    private static class CompactJsonWriter extends MapWrappingWriter
    {
        private final MappingTemplate template;

        public CompactJsonWriter( MappingTemplate template )
        {
            super( new HashMap<String, Object>(), true );
            this.template = template;
        }

        @Override
        protected MappingWriter newMapping( String type, String key )
        {
            Map<String, Object> map = new HashMap<String, Object>();
            data.put( key, map );
            return new MapWrappingWriter( map, interactive );
        }

        @Override
        protected void writeValue( String type, String key, Object value )
        {
            data.put( key, value );
        }

        @Override
        protected ListWriter newList( String type, String key )
        {
            List<Object> list = new ArrayList<Object>();
            data.put( key, list );
            return new ListWrappingWriter( list, interactive );
        }

        String complete()
        {
            return template.render( this.data );
        }

    }

    @Override
    protected ListWriter serializeList( String type )
    {
        return new ListWrappingWriter( new ArrayList<Object>() );
    }

    @Override
    protected String complete( ListWriter serializer )
    {
        return JsonHelper.createJsonFrom( ( (ListWrappingWriter) serializer ).data );
    }

    @Override
    protected MappingWriter serializeMapping( String type )
    {
        MappingTemplate template = MappingTemplate.TEMPLATES.get( type );
        if ( template == null )
        {
            throw new WebApplicationException( Response.status( Response.Status.NOT_ACCEPTABLE )
                    .entity( "Cannot represent \"" + type + "\" as compactJson" )
                    .build() );
        }
        return new CompactJsonWriter( template );
    }

    @Override
    protected String complete( MappingWriter serializer )
    {
        return ( (CompactJsonWriter) serializer ).complete();
    }

    @Override
    protected String serializeValue( String type, Object value )
    {
        return JsonHelper.createJsonFrom( value );
    }

    private boolean empty( String input )
    {
        return input == null || "".equals( input.trim() );
    }

    @Override
    public Map<String, Object> readMap( String input, String... requiredKeys ) throws BadInputException
    {
        if ( empty( input ) ) return DefaultFormat.validateKeys( Collections.<String,Object>emptyMap(), requiredKeys );
        try
        {
            return DefaultFormat.validateKeys( JsonHelper.jsonToMap( stripByteOrderMark( input ) ), requiredKeys );
        }
        catch ( JsonParseException ex )
        {
            throw new BadInputException( ex );
        }
    }

    @Override
    public List<Object> readList( String input )
    {
        // TODO tobias: Implement readList() [Dec 10, 2010]
        throw new UnsupportedOperationException( "Not implemented: JsonInput.readList()" );
    }

    @Override
    public Object readValue( String input ) throws BadInputException
    {
        if ( empty( input ) ) return Collections.emptyMap();
        try
        {
            return assertSupportedPropertyValue( readJson( stripByteOrderMark( input ) ) );
        }
        catch ( JsonParseException ex )
        {
            throw new BadInputException( ex );
        }
    }

    @Override
    public URI readUri( String input ) throws BadInputException
    {
        try
        {
            return new URI( readValue( input ).toString() );
        }
        catch ( URISyntaxException e )
        {
            throw new BadInputException( e );
        }
    }

    private String stripByteOrderMark( String string )
    {
        if ( string != null && string.length() > 0 && string.charAt( 0 ) == 0xfeff )
        {
            return string.substring( 1 );
        }
        return string;
    }
}
