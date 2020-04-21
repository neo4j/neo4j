/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.http.cypher.format.output.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.DefaultFormat;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.RepresentationFormat;
import org.neo4j.server.rest.repr.StreamingFormat;

import static com.fasterxml.jackson.databind.SerializationFeature.FLUSH_AFTER_WRITE_VALUE;
import static org.neo4j.server.rest.domain.JsonHelper.assertSupportedPropertyValue;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;

@ServiceProvider
public class StreamingJsonFormat extends RepresentationFormat implements StreamingFormat
{

    private final JsonFactory factory;

    public StreamingJsonFormat()
    {
        super( MEDIA_TYPE );
        this.factory = createJsonFactory();
    }

    private JsonFactory createJsonFactory()
    {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable( FLUSH_AFTER_WRITE_VALUE );
        JsonFactory factory = new JsonFactory( objectMapper )
        {
            @Override
            protected JsonGenerator _createUTF8Generator( OutputStream out, IOContext ctxt )
            {
                final int bufferSize = 1024 * 8;
                UTF8JsonGenerator gen = new UTF8JsonGenerator( ctxt, _generatorFeatures, _objectCodec, out, DEFAULT_QUOTE_CHAR,
                        new byte[bufferSize], 0, true );
                if ( _characterEscapes != null )
                {
                    gen.setCharacterEscapes( _characterEscapes );
                }
                return gen;
            }
        };
        factory.disable( JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM );
        return factory;
    }

    @Override
    public StreamingRepresentationFormat writeTo( OutputStream output )
    {
        try
        {
            JsonGenerator g = factory.createGenerator( output );
            return new StreamingRepresentationFormat( g, this );
        }
        catch ( IOException e )
        {
            throw new WebApplicationException( e );
        }
    }

    @Override
    protected ListWriter serializeList( String type )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String complete( ListWriter serializer )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected MappingWriter serializeMapping( String type )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String complete( MappingWriter serializer )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String serializeValue( String type, Object value )
    {
        throw new UnsupportedOperationException();
    }

    private boolean empty( String input )
    {
        return input == null || "".equals( input.trim() );
    }

    @Override
    public Map<String, Object> readMap( String input, String... requiredKeys ) throws BadInputException
    {
        if ( empty( input ) )
        {
            return DefaultFormat.validateKeys( Collections.emptyMap(), requiredKeys );
        }
        try
        {
            return DefaultFormat.validateKeys( JsonHelper.jsonToMap( stripByteOrderMark( input ) ), requiredKeys );
        }
        catch ( Exception ex )
        {
            throw new BadInputException( ex );
        }
    }

    @Override
    public List<Object> readList( String input )
    {
        throw new UnsupportedOperationException( "Not implemented: JsonInput.readList()" );
    }

    @Override
    public Object readValue( String input ) throws BadInputException
    {
        if ( empty( input ) )
        {
            return Collections.emptyMap();
        }
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
        if ( string != null && !string.isEmpty() && string.charAt( 0 ) == 0xfeff )
        {
            return string.substring( 1 );
        }
        return string;
    }

    private static class StreamingMappingWriter extends MappingWriter
    {
        private final JsonGenerator g;

        StreamingMappingWriter( JsonGenerator g )
        {
            this.g = g;
            try
            {
                g.writeStartObject();
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        StreamingMappingWriter( JsonGenerator g, String key )
        {
            this.g = g;
            try
            {
                g.writeObjectFieldStart( key );
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        @Override
        public MappingWriter newMapping( String type, String key )
        {
            return new StreamingMappingWriter( g, key );
        }

        @Override
        public ListWriter newList( String type, String key )
        {
            return new StreamingListWriter( g, key );
        }

        @Override
        public void writeValue( String type, String key, Object value )
        {
            try
            {
                g.writeObjectField( key, value ); // todo individual fields
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        @Override
        public void done()
        {
            try
            {
                g.writeEndObject();
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }
    }

    private static class StreamingListWriter extends ListWriter
    {
        private final JsonGenerator g;

        StreamingListWriter( JsonGenerator g )
        {
            this.g = g;
            try
            {
                g.writeStartArray();
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        StreamingListWriter( JsonGenerator g, String key )
        {
            this.g = g;
            try
            {
                g.writeArrayFieldStart( key );
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        @Override
        public MappingWriter newMapping( String type )
        {
            return new StreamingMappingWriter( g );
        }

        @Override
        public ListWriter newList( String type )
        {
            return new StreamingListWriter( g );
        }

        @Override
        public void writeValue( String type, Object value )
        {
            try
            {
                g.writeObject( value );
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        @Override
        public void done()
        {
            try
            {
                g.writeEndArray();
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

    }

    public static class StreamingRepresentationFormat extends RepresentationFormat
    {
        private final JsonGenerator g;
        private final InputFormat inputFormat;

        public StreamingRepresentationFormat( JsonGenerator g, InputFormat inputFormat )
        {
            super( StreamingFormat.MEDIA_TYPE );
            this.g = g;
            this.inputFormat = inputFormat;
        }

        public StreamingRepresentationFormat usePrettyPrinter()
        {
            g.useDefaultPrettyPrinter();
            return this;
        }

        @Override
        protected String serializeValue( String type, Object value )
        {
            try
            {
                g.writeObject( value );
                return null;
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        @Override
        protected ListWriter serializeList( String type )
        {
            return new StreamingListWriter( g );
        }

        @Override
        public MappingWriter serializeMapping( String type )
        {
            return new StreamingMappingWriter( g );
        }

        @Override
        protected String complete( ListWriter serializer )
        {
            flush();
            return null; // already done in done()
        }

        @Override
        protected String complete( MappingWriter serializer )
        {
            flush();
            return null;  // already done in done()
        }

        private void flush()
        {
            try
            {
                g.flush();
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }

        @Override
        public Object readValue( String input ) throws BadInputException
        {
            return inputFormat.readValue( input );
        }

        @Override
        public Map<String, Object> readMap( String input, String... requiredKeys ) throws BadInputException
        {
            return inputFormat.readMap( input, requiredKeys );
        }

        @Override
        public List<Object> readList( String input ) throws BadInputException
        {
            return inputFormat.readList( input );
        }

        @Override
        public URI readUri( String input ) throws BadInputException
        {
            return inputFormat.readUri( input );
        }

        @Override
        public void complete()
        {
            try
            {
                g.flush();
            }
            catch ( IOException e )
            {
                throw new WebApplicationException( e );
            }
        }
    }
}
