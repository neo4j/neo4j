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

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.server.helpers.PropertyTypeDispatcher;

public final class PropertiesRepresentation extends MappingRepresentation
{
    private final PropertyContainer entity;

    public PropertiesRepresentation( PropertyContainer entity )
    {
        super( RepresentationType.PROPERTIES );
        this.entity = entity;
    }

    public boolean isEmpty()
    {
        return !entity.getPropertyKeys()
                .iterator()
                .hasNext();
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serialize( serializer.writer );
    }

    void serialize( MappingWriter writer )
    {
        PropertyTypeDispatcher.consumeProperties( new Consumer( writer ), entity );
    }

    private static class Consumer extends PropertyTypeDispatcher<String, Void>
    {
        private final MappingWriter writer;

        Consumer( MappingWriter serializer )
        {
            this.writer = serializer;
        }

        @Override
        protected Void dispatchBooleanProperty( boolean property, String param )
        {
            writer.writeBoolean( param, property );
            return null;
        }

        @Override
        protected Void dispatchByteProperty( byte property, String param )
        {
            writer.writeInteger( RepresentationType.BYTE, param, property );
            return null;
        }

        @Override
        protected Void dispatchCharacterProperty( char property, String param )
        {
            writer.writeInteger( RepresentationType.CHAR, param, property );
            return null;
        }

        @Override
        protected Void dispatchDoubleProperty( double property, String param )
        {
            writer.writeFloatingPointNumber( RepresentationType.DOUBLE, param, property );
            return null;
        }

        @Override
        protected Void dispatchFloatProperty( float property, String param )
        {
            writer.writeFloatingPointNumber( RepresentationType.FLOAT, param, property );
            return null;
        }

        @Override
        protected Void dispatchIntegerProperty( int property, String param )
        {
            writer.writeInteger( RepresentationType.INTEGER, param, property );
            return null;
        }

        @Override
        protected Void dispatchLongProperty( long property, String param )
        {
            writer.writeInteger( RepresentationType.LONG, param, property );
            return null;
        }

        @Override
        protected Void dispatchShortProperty( short property, String param )
        {
            writer.writeInteger( RepresentationType.SHORT, param, property );
            return null;
        }

        @Override
        protected Void dispatchStringProperty( String property, String param )
        {
            writer.writeString( param, property );
            return null;
        }

        @Override
        protected Void dispatchStringArrayProperty( String[] property, String param )
        {
            ListWriter list = writer.newList( RepresentationType.STRING, param );
            for ( String s : property )
            {
                list.writeString( s );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchByteArrayProperty( PropertyArray<byte[], Byte> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.BYTE, param );
            for ( Byte b : array )
            {
                list.writeInteger( RepresentationType.BYTE, b );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchShortArrayProperty( PropertyArray<short[], Short> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.SHORT, param );
            for ( Short s : array )
            {
                list.writeInteger( RepresentationType.SHORT, s );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchCharacterArrayProperty( PropertyArray<char[], Character> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.CHAR, param );
            for ( Character c : array )
            {
                list.writeInteger( RepresentationType.CHAR, c );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchIntegerArrayProperty( PropertyArray<int[], Integer> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.INTEGER, param );
            for ( Integer i : array )
            {
                list.writeInteger( RepresentationType.INTEGER, i );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchLongArrayProperty( PropertyArray<long[], Long> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.LONG, param );
            for ( Long j : array )
            {
                list.writeInteger( RepresentationType.LONG, j );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchFloatArrayProperty( PropertyArray<float[], Float> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.FLOAT, param );
            for ( Float f : array )
            {
                list.writeFloatingPointNumber( RepresentationType.FLOAT, f );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchDoubleArrayProperty( PropertyArray<double[], Double> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.DOUBLE, param );
            for ( Double d : array )
            {
                list.writeFloatingPointNumber( RepresentationType.DOUBLE, d );
            }
            list.done();
            return null;
        }

        @Override
        @SuppressWarnings( "boxing" )
        protected Void dispatchBooleanArrayProperty( PropertyArray<boolean[], Boolean> array, String param )
        {
            ListWriter list = writer.newList( RepresentationType.BOOLEAN, param );
            for ( Boolean z : array )
            {
                list.writeBoolean( z );
            }
            list.done();
            return null;
        }
    }

    public static Representation value( Object property )
    {
        return ValueRepresentation.property( property );
    }
}
