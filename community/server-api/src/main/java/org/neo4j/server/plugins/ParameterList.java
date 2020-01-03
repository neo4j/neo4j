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
package org.neo4j.server.plugins;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.BadInputException;

/**
 * @deprecated Server plugins are deprecated for removal in the next major release. Please use unmanaged extensions instead.
 */
@Deprecated
public abstract class ParameterList
{
    private final Map<String, Object> data;

    public ParameterList( Map<String, Object> data )
    {
        this.data = data;
    }

    private abstract static class Converter<T>
    {
        abstract T convert( GraphDatabaseAPI graphDb, Object value )
                throws BadInputException;

        abstract T[] newArray( int size );
    }

    private <T> T[] getList( String name, GraphDatabaseAPI graphDb,
            Converter<T> converter ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        List<T> result = new ArrayList<>();
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

    @Deprecated
    public String getString( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertString( value );
    }

    @Deprecated
    public String[] getStringList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<String>()
        {
            @Override
            String convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract String convertString( Object value )
            throws BadInputException;

    @Deprecated
    public Integer getInteger( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertInteger( value );
    }

    @Deprecated
    public Integer[] getIntegerList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Integer>()
        {
            @Override
            Integer convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Integer convertInteger( Object value )
            throws BadInputException;

    @Deprecated
    public Long getLong( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertLong( value );
    }

    @Deprecated
    public Long[] getLongList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Long>()
        {
            @Override
            Long convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Long convertLong( Object value )
            throws BadInputException;

    @Deprecated
    public Byte getByte( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertByte( value );
    }

    @Deprecated
    public Byte[] getByteList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Byte>()
        {
            @Override
            Byte convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Byte convertByte( Object value )
            throws BadInputException;

    @Deprecated
    public Character getCharacter( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertCharacter( value );
    }

    @Deprecated
    public Character[] getCharacterList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Character>()
        {
            @Override
            Character convert( GraphDatabaseAPI graphDb, Object value )
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

    @Deprecated
    protected abstract Character convertCharacter( Object value )
            throws BadInputException;

    @Deprecated
    public Boolean getBoolean( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertBoolean( value );
    }

    @Deprecated
    public Boolean[] getBooleanList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Boolean>()
        {
            @Override
            Boolean convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Boolean convertBoolean( Object value )
            throws BadInputException;

    @Deprecated
    public Short getShort( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertShort( value );
    }

    @Deprecated
    public Short[] getShortList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Short>()
        {
            @Override
            Short convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Short convertShort( Object value )
            throws BadInputException;

    @Deprecated
    public Float getFloat( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertFloat( value );
    }

    @Deprecated
    public Float[] getFloatList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Float>()
        {
            @Override
            Float convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Float convertFloat( Object value )
            throws BadInputException;

    @Deprecated
    public Double getDouble( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertDouble( value );
    }

    @Deprecated
    public Double[] getDoubleList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<Double>()
        {
            @Override
            Double convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Double convertDouble( Object value )
            throws BadInputException;

    @Deprecated
    public Node getNode( GraphDatabaseAPI graphDb, String name )
            throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertNode( graphDb, value );
    }

    @Deprecated
    public Node[] getNodeList( GraphDatabaseAPI graphDb, String name )
            throws BadInputException
    {
        return getList( name, graphDb, new Converter<Node>()
        {
            @Override
            Node convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
    protected abstract Node convertNode( GraphDatabaseAPI graphDb, Object value ) throws BadInputException;

    @Deprecated
    public Relationship getRelationship( GraphDatabaseAPI graphDb, String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertRelationship( graphDb, value );
    }

    @Deprecated
    public Relationship[] getRelationshipList( GraphDatabaseAPI graphDb, String name ) throws BadInputException
    {
        return getList( name, graphDb, new Converter<Relationship>()
        {
            @Override
            Relationship convert( GraphDatabaseAPI graphDb, Object value )
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

    @Deprecated
    protected abstract Relationship convertRelationship( GraphDatabaseAPI graphDb, Object value )
            throws BadInputException;

    @Deprecated
    public URI getUri( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value == null )
        {
            return null;
        }
        return convertURI( value );
    }

    @Deprecated
    public URI[] getUriList( String name ) throws BadInputException
    {
        return getList( name, null, new Converter<URI>()
        {
            @Override
            URI convert( GraphDatabaseAPI graphDb, Object value )
                    throws BadInputException
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

    @Deprecated
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

    @Deprecated
    public Map getMap( String name ) throws BadInputException
    {
        Object value = data.get( name );
        if ( value instanceof Map )
        {
            return (Map) value;
        }
        else if ( value instanceof String )
        {
            throw new BadInputException( "Maps encoded as Strings not supported" );
        }
        return null;
    }
}
