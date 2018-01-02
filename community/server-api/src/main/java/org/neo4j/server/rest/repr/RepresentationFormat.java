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
package org.neo4j.server.rest.repr;

import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.core.MediaType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.plugins.ParameterList;
import org.neo4j.server.rest.web.NodeNotFoundException;
import org.neo4j.server.rest.web.RelationshipNotFoundException;

/**
 * Implementations of this class must be stateless. Implementations of this
 * class must have a public no arguments constructor.
 */
public abstract class RepresentationFormat implements InputFormat
{
    final MediaType mediaType;

    public RepresentationFormat( MediaType mediaType )
    {
        this.mediaType = mediaType;
    }

    @Override
    public String toString()
    {
        return String.format( "%s[%s]", getClass().getSimpleName(), mediaType );
    }

    String serializeValue( RepresentationType type, Object value )
    {
        return serializeValue( type.valueName, value );
    }

    protected abstract String serializeValue( String type, Object value );

    ListWriter serializeList( RepresentationType type )
    {
        if ( type.listName == null )
            throw new IllegalStateException( "Invalid list type: " + type );
        return serializeList( type.listName );
    }

    protected abstract ListWriter serializeList( String type );

    MappingWriter serializeMapping( RepresentationType type )
    {
        return serializeMapping( type.valueName );
    }

    protected abstract MappingWriter serializeMapping( String type );

    /**
     * Will be invoked (when serialization is done) with the result retrieved
     * from invoking {@link #serializeList(String)}, it is therefore safe for
     * this method to convert the {@link ListWriter} argument to the
     * implementation class returned by {@link #serializeList(String)}.
     */
    protected abstract String complete( ListWriter serializer ) ;

    /**
     * Will be invoked (when serialization is done) with the result retrieved
     * from invoking {@link #serializeMapping(String)}, it is therefore safe for
     * this method to convert the {@link MappingWriter} argument to the
     * implementation class returned by {@link #serializeMapping(String)}.
     */
    protected abstract String complete( MappingWriter serializer ) ;

    @Override
    public ParameterList readParameterList( String input ) throws BadInputException
    {
        return new ParameterList( readMap( input ) )
        {
            @Override
            protected Boolean convertBoolean( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertBoolean( value );
            }

            @Override
            protected Byte convertByte( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertByte( value );
            }

            @Override
            protected Character convertCharacter( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertCharacter( value );
            }

            @Override
            protected Double convertDouble( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertDouble( value );
            }

            @Override
            protected Float convertFloat( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertFloat( value );
            }

            @Override
            protected Integer convertInteger( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertInteger( value );
            }

            @Override
            protected Long convertLong( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertLong( value );
            }

            @Override
            protected Node convertNode( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
            {
                return RepresentationFormat.this.convertNode( graphDb, value );
            }

            @Override
            protected Relationship convertRelationship( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
            {
                return RepresentationFormat.this.convertRelationship( graphDb, value );
            }

            @Override
            protected Short convertShort( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertShort( value );
            }

            @Override
            protected String convertString( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertString( value );
            }

            @Override
            protected URI convertURI( Object value ) throws BadInputException
            {
                return RepresentationFormat.this.convertURI( value );
            }
        };
    }

    protected Relationship convertRelationship( GraphDatabaseAPI graphDb, Object value )
            throws BadInputException
    {
        if ( value instanceof Relationship )
        {
            return (Relationship) value;
        }
        if ( value instanceof URI )
        {
            try
            {
                return getRelationship( graphDb, (URI) value );
            }
            catch ( RelationshipNotFoundException e )
            {
                throw new BadInputException( e );
            }
        }
        if ( value instanceof String )
        {
            try
            {
                return getRelationship( graphDb, (String) value );
            }
            catch ( RelationshipNotFoundException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    protected Node convertNode( GraphDatabaseAPI graphDb, Object value )
            throws BadInputException
    {
        if ( value instanceof Node )
        {
            return (Node) value;
        }
        if ( value instanceof URI )
        {
            try
            {
                return getNode( graphDb, (URI) value );
            }
            catch ( NodeNotFoundException e )
            {
                throw new BadInputException( e );
            }
        }
        if ( value instanceof String )
        {
            try
            {
                return getNode( graphDb, (String) value );
            }
            catch ( NodeNotFoundException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    protected Node getNode( GraphDatabaseAPI graphDb, String value ) throws BadInputException,
            NodeNotFoundException
    {
        try
        {
            return getNode( graphDb, new URI( value ) );
        }
        catch ( URISyntaxException e )
        {
            throw new BadInputException( e );
        }
    }

    protected Node getNode( GraphDatabaseAPI graphDb, URI uri ) throws BadInputException,
            NodeNotFoundException
    {
        try
        {
            return graphDb.getNodeById( extractId( uri ) );
        }
        catch ( NotFoundException e )
        {
            throw new NodeNotFoundException(e);
        }
    }

    private long extractId( URI uri ) throws BadInputException
    {
        String[] path = uri.getPath().split( "/" );
        try
        {
            return Long.parseLong( path[path.length - 1] );
        }
        catch ( NumberFormatException e )
        {
            throw new BadInputException( e );
        }
    }

    private Relationship getRelationship( GraphDatabaseAPI graphDb, String value )
            throws BadInputException, RelationshipNotFoundException
    {
        try
        {
            return getRelationship( graphDb, new URI( value ) );
        }
        catch ( URISyntaxException e )
        {
            throw new BadInputException( e );
        }
    }

    protected Relationship getRelationship( GraphDatabaseAPI graphDb, URI uri )
            throws BadInputException, RelationshipNotFoundException
    {
        try
        {
            return graphDb.getRelationshipById( extractId( uri ) );
        }
        catch ( NotFoundException e )
        {
            throw new RelationshipNotFoundException( e );
        }
    }

    protected URI convertURI( Object value ) throws BadInputException
    {
        if ( value instanceof URI )
        {
            return (URI) value;
        }
        if ( value instanceof String )
        {
            try
            {
                return new URI( (String) value );
            }
            catch ( URISyntaxException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    protected String convertString( Object value ) throws BadInputException
    {
        if ( value instanceof String )
        {
            return (String) value;
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Short convertShort( Object value ) throws BadInputException
    {
        if ( value instanceof Number && !( value instanceof Float || value instanceof Double ) )
        {
            short primitive = ( (Number) value ).shortValue();
            if ( primitive != ( (Number) value ).longValue() )
                throw new BadInputException( "Input did not fit in short" );
            return primitive;
        }
        if ( value instanceof String )
        {
            try
            {
                return Short.parseShort( (String) value );
            }
            catch ( NumberFormatException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Long convertLong( Object value ) throws BadInputException
    {
        if ( value instanceof Number && !( value instanceof Float || value instanceof Double ) )
        {
            long primitive = ( (Number) value ).longValue();
            return primitive;
        }
        if ( value instanceof String )
        {
            try
            {
                return Long.parseLong( (String) value );
            }
            catch ( NumberFormatException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Integer convertInteger( Object value ) throws BadInputException
    {
        if ( value instanceof Number && !( value instanceof Float || value instanceof Double ) )
        {
            int primitive = ( (Number) value ).intValue();
            if ( primitive != ( (Number) value ).longValue() )
                throw new BadInputException( "Input did not fit in int" );
            return primitive;
        }
        if ( value instanceof String )
        {
            try
            {
                return Integer.parseInt( (String) value );
            }
            catch ( NumberFormatException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Float convertFloat( Object value ) throws BadInputException
    {
        if ( value instanceof Number )
        {
            return ( (Number) value ).floatValue();
        }
        if ( value instanceof String )
        {
            try
            {
                return Float.parseFloat( (String) value );
            }
            catch ( NumberFormatException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Double convertDouble( Object value ) throws BadInputException
    {
        if ( value instanceof Number )
        {
            return ( (Number) value ).doubleValue();
        }
        if ( value instanceof String )
        {
            try
            {
                return Double.parseDouble( (String) value );
            }
            catch ( NumberFormatException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Character convertCharacter( Object value ) throws BadInputException
    {
        if ( value instanceof Character )
        {
            return (Character) value;
        }
        if ( value instanceof Number )
        {
            int primitive = ( (Number) value ).intValue();
            if ( primitive != ( (Number) value ).longValue() || ( primitive > 0xFFFF ) )
                throw new BadInputException( "Input did not fit in char" );
            return Character.valueOf( (char) primitive );
        }
        if ( value instanceof String && ( (String) value ).length() == 1 )
        {
            return ( (String) value ).charAt( 0 );
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Byte convertByte( Object value ) throws BadInputException
    {
        if ( value instanceof Number )
        {
            byte primitive = ( (Number) value ).byteValue();
            if ( primitive != ( (Number) value ).longValue() )
                throw new BadInputException( "Input did not fit in byte" );
            return primitive;
        }
        if ( value instanceof String )
        {
            try
            {
                return Byte.parseByte( (String) value );
            }
            catch ( NumberFormatException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    @SuppressWarnings( "boxing" )
    protected Boolean convertBoolean( Object value ) throws BadInputException
    {
        if ( value instanceof Boolean )
        {
            return (Boolean) value;
        }
        if ( value instanceof String )
        {
            try
            {
                return Boolean.parseBoolean( (String) value );
            }
            catch ( NumberFormatException e )
            {
                throw new BadInputException( e );
            }
        }
        throw new BadInputException( "Could not convert!" );
    }

    public void complete() {
    }
}
