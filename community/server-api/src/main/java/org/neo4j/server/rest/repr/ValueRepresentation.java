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

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.server.helpers.PropertyTypeDispatcher;

public class ValueRepresentation extends Representation
{
    private final Object value;

    private ValueRepresentation( RepresentationType type, Object value )
    {
        super( type );
        this.value = value;
    }

    @Override
    String serialize( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
    {
        final String result = format.serializeValue(type, value);
        format.complete();
        return result;
    }

    @Override
    void addTo( ListSerializer serializer )
    {
        serializer.writer.writeValue( type, value );
    }

    @Override
    void putTo( MappingSerializer serializer, String key )
    {
        serializer.writer.writeValue( type, key, value );
    }

    public static ValueRepresentation ofNull()
    {
        return new ValueRepresentation( RepresentationType.NULL, null );
    }

    public static ValueRepresentation string( String value )
    {
        return new ValueRepresentation( RepresentationType.STRING, value );
    }

    @SuppressWarnings( "boxing" )
    public static ValueRepresentation number( int value )
    {
        return new ValueRepresentation( RepresentationType.INTEGER, value );
    }

    @SuppressWarnings( "boxing" )
    public static ValueRepresentation number( long value )
    {
        return new ValueRepresentation( RepresentationType.LONG, value );
    }

    @SuppressWarnings( "boxing" )
    public static ValueRepresentation number( double value )
    {
        return new ValueRepresentation( RepresentationType.DOUBLE, value );
    }

    public static ValueRepresentation bool( boolean value )
    {
        return new ValueRepresentation( RepresentationType.BOOLEAN, value );
    }

    public static ValueRepresentation relationshipType( RelationshipType type )
    {
        return new ValueRepresentation( RepresentationType.RELATIONSHIP_TYPE, type.name() );
    }

    public static ValueRepresentation uri( final String path )
    {
        return new ValueRepresentation( RepresentationType.URI, null )
        {
            @Override
            String serialize( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
            {
                return Serializer.joinBaseWithRelativePath( baseUri, path );
            }

            @Override
            void addTo( ListSerializer serializer )
            {
                serializer.addUri( path );
            }

            @Override
            void putTo( MappingSerializer serializer, String key )
            {
                serializer.putUri( key, path );
            }
        };
    }

    public static ValueRepresentation template( final String path )
    {
        return new ValueRepresentation( RepresentationType.TEMPLATE, null )
        {
            @Override
            String serialize( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
            {
                return Serializer.joinBaseWithRelativePath( baseUri, path );
            }

            @Override
            void addTo( ListSerializer serializer )
            {
                serializer.addUriTemplate( path );
            }

            @Override
            void putTo( MappingSerializer serializer, String key )
            {
                serializer.putUriTemplate( key, path );
            }
        };
    }

    static Representation property( Object property )
    {
        return PROPERTY_REPRESENTATION.dispatch( property, null );
    }

    private static final PropertyTypeDispatcher<Void, Representation> PROPERTY_REPRESENTATION = new PropertyTypeDispatcher<Void, Representation>()
    {
        @Override
        protected Representation dispatchBooleanProperty( boolean property, Void param )
        {
            return bool( property );
        }

        @Override
        protected Representation dispatchByteProperty( byte property, Void param )
        {
            return new ValueRepresentation( RepresentationType.BYTE, property );
        }

        @Override
        protected Representation dispatchCharacterProperty( char property, Void param )
        {
            return new ValueRepresentation( RepresentationType.CHAR, property );
        }

        @Override
        protected Representation dispatchDoubleProperty( double property, Void param )
        {
            return new ValueRepresentation( RepresentationType.DOUBLE, property );
        }

        @Override
        protected Representation dispatchFloatProperty( float property, Void param )
        {
            return new ValueRepresentation( RepresentationType.FLOAT, property );
        }

        @Override
        protected Representation dispatchIntegerProperty( int property, Void param )
        {
            return new ValueRepresentation( RepresentationType.INTEGER, property );
        }

        @Override
        protected Representation dispatchLongProperty( long property, Void param )
        {
            return new ValueRepresentation( RepresentationType.LONG, property );
        }

        @Override
        protected Representation dispatchShortProperty( short property, Void param )
        {
            return new ValueRepresentation( RepresentationType.SHORT, property );
        }

        @Override
        protected Representation dispatchStringProperty( String property, Void param )
        {
            return string( property );
        }

        @Override
        protected Representation dispatchStringArrayProperty( String[] property, Void param )
        {
            return ListRepresentation.strings( property );
        }

        @SuppressWarnings( "unchecked" )
        private Iterable<Representation> dispatch( PropertyArray<?, ?> array )
        {
            return new IterableWrapper<Representation, Object>( (Iterable<Object>) array )
            {
                @Override
                protected Representation underlyingObjectToObject( Object object )
                {
                    return property( object );
                }
            };
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchByteArrayProperty( PropertyArray<byte[], Byte> array,
                Void param )
        {
            return toListRepresentation(RepresentationType.BYTE, array);
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchShortArrayProperty( PropertyArray<short[], Short> array,
                Void param )
        {
            return toListRepresentation(RepresentationType.SHORT, array);
        }

        private ListRepresentation toListRepresentation(RepresentationType type, PropertyArray<?, ?> array) {
            return new ListRepresentation(type, dispatch(array) );
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchCharacterArrayProperty(
                PropertyArray<char[], Character> array, Void param )
        {
            return toListRepresentation(RepresentationType.CHAR, array);
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchIntegerArrayProperty( PropertyArray<int[], Integer> array,
                Void param )
        {
            return toListRepresentation(RepresentationType.INTEGER, array);
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchLongArrayProperty( PropertyArray<long[], Long> array,
                Void param )
        {
            return toListRepresentation(RepresentationType.LONG, array);
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchFloatArrayProperty( PropertyArray<float[], Float> array,
                Void param )
        {
            return toListRepresentation(RepresentationType.FLOAT, array);
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchDoubleArrayProperty(
                PropertyArray<double[], Double> array, Void param )
        {
            return toListRepresentation(RepresentationType.DOUBLE, array);
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Representation dispatchBooleanArrayProperty(
                PropertyArray<boolean[], Boolean> array, Void param )
        {
            return toListRepresentation(RepresentationType.BOOLEAN, array);
        }
    };
}
