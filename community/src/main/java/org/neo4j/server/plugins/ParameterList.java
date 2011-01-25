/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.plugins;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.rest.repr.BadInputException;

public abstract class ParameterList
{
    private final Map<String, Object> data;

    public ParameterList( Map<String, Object> data )
    {
        this.data = data;
    }

    public Map<String, Object> asMap()
    {
        return Collections.unmodifiableMap( data );
    }

    private static abstract class Converter<T> {
        abstract T convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException;

        abstract T[] newArray( int size );
    }

    private <T> T[] getList( String name, AbstractGraphDatabase graphDb, Converter<T> converter )
            throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        List<T> result = new ArrayList<T>();
        if ( value instanceof Object[] )
        {
            for ( Object element : (Object[]) value )
            {
                result.add( converter.convert( graphDb, element ) );
            }
        }
        else if ( value instanceof Iterable<?> )
        {
            for ( Object element : (Iterable<?>) value )
            {
                result.add( converter.convert( graphDb, element ) );
            }
        }
        else
        {
            throw new BadInputException( name + " is not a list" );
        }
        return result.toArray( converter.newArray( result.size() ) );
    }

    public String getString( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertString( value );
    }

    public String[] getStringList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<String>()
        {
            @Override
            String convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertString( value );
            }

            @Override
            String[] newArray( int size )
            {
                return new String[size];
            }
        } );
    }

    protected abstract String convertString( Object value ) throws BadInputException;

    public Integer getInteger( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertInteger( value );
    }

    public Integer[] getIntegerList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Integer>()
        {
            @Override
            Integer convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertInteger( value );
            }

            @Override
            Integer[] newArray( int size )
            {
                return new Integer[size];
            }
        } );
    }

    protected abstract Integer convertInteger( Object value ) throws BadInputException;

    public Long getLong( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertLong( value );
    }

    public Long[] getLongList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Long>()
        {
            @Override
            Long convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertLong( value );
            }

            @Override
            Long[] newArray( int size )
            {
                return new Long[size];
            }
        } );
    }

    protected abstract Long convertLong( Object value ) throws BadInputException;

    public Byte getByte( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertByte( value );
    }

    public Byte[] getByteList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Byte>()
        {
            @Override
            Byte convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertByte( value );
            }

            @Override
            Byte[] newArray( int size )
            {
                return new Byte[size];
            }
        } );
    }

    protected abstract Byte convertByte( Object value ) throws BadInputException;

    public Character getCharacter( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertCharacter( value );
    }

    public Character[] getCharacterList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Character>()
        {
            @Override
            Character convert( AbstractGraphDatabase graphDb, Object value )
                    throws BadInputException
            {
                return convertCharacter( value );
            }

            @Override
            Character[] newArray( int size )
            {
                return new Character[size];
            }
        } );
    }

    protected abstract Character convertCharacter( Object value ) throws BadInputException;

    public Boolean getBoolean( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertBoolean( value );
    }

    public Boolean[] getBooleanList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Boolean>()
        {
            @Override
            Boolean convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertBoolean( value );
            }

            @Override
            Boolean[] newArray( int size )
            {
                return new Boolean[size];
            }
        } );
    }

    protected abstract Boolean convertBoolean( Object value ) throws BadInputException;

    public Short getShort( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertShort( value );
    }

    public Short[] getShortList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Short>()
        {
            @Override
            Short convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertShort( value );
            }

            @Override
            Short[] newArray( int size )
            {
                return new Short[size];
            }
        } );
    }

    protected abstract Short convertShort( Object value ) throws BadInputException;

    public Float getFloat( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertFloat( value );
    }

    public Float[] getFloatList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Float>()
        {
            @Override
            Float convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertFloat( value );
            }

            @Override
            Float[] newArray( int size )
            {
                return new Float[size];
            }
        } );
    }

    protected abstract Float convertFloat( Object value ) throws BadInputException;

    public Double getDouble( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertDouble( value );
    }

    public Double[] getDoubleList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Double>()
        {
            @Override
            Double convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertDouble( value );
            }

            @Override
            Double[] newArray( int size )
            {
                return new Double[size];
            }
        } );
    }

    protected abstract Double convertDouble( Object value ) throws BadInputException;

    public Node getNode( AbstractGraphDatabase graphDb, String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertNode( graphDb, value );
    }

    public Node[] getNodeList( AbstractGraphDatabase graphDb, String name )
            throws BadInputException
    {
        return getList( name, graphDb, new Converter<Node>()
        {
            @Override
            Node convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertNode( graphDb, value );
            }

            @Override
            Node[] newArray( int size )
            {
                return new Node[size];
            }
        } );
    }

    protected abstract Node convertNode( AbstractGraphDatabase graphDb, Object value )
            throws BadInputException;

    public Relationship getRelationship( AbstractGraphDatabase graphDb, String name )
            throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertRelationship( graphDb, value );
    }

    public Relationship[] getRelationshipList( AbstractGraphDatabase graphDb, String name )
            throws BadInputException
    {
        return getList( name, graphDb, new Converter<Relationship>()
        {
            @Override
            Relationship convert( AbstractGraphDatabase graphDb, Object value )
                    throws BadInputException
            {
                return convertRelationship( graphDb, value );
            }

            @Override
            Relationship[] newArray( int size )
            {
                return new Relationship[size];
            }
        } );
    }

    protected abstract Relationship convertRelationship( AbstractGraphDatabase graphDb, Object value )
            throws BadInputException;

    public URI getUri( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null ) return null;
        return convertURI( value );
    }

    public URI[] getUriList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<URI>()
        {
            @Override
            URI convert( AbstractGraphDatabase graphDb, Object value ) throws BadInputException
            {
                return convertURI( value );
            }

            @Override
            URI[] newArray( int size )
            {
                return new URI[size];
            }
        } );
    }

    protected URI convertURI( Object value ) throws BadInputException
    {
        try
        {
            return new URI( convertString( value ) );
        }
        catch ( URISyntaxException e )
        {
            throw new BadInputException( e );
        }
    }
}
