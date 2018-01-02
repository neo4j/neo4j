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

import java.util.ArrayList;

import org.neo4j.server.helpers.PropertyTypeDispatcher;

import static org.neo4j.server.rest.repr.ValueRepresentation.bool;
import static org.neo4j.server.rest.repr.ValueRepresentation.number;
import static org.neo4j.server.rest.repr.ValueRepresentation.string;

/**
 * Converts common primitive and basic objects and arrays of the same into a
 * representation. Handy for specialization.
 *
 * @see org.neo4j.server.rest.management.repr.JmxAttributeRepresentationDispatcher
 */
public abstract class RepresentationDispatcher extends PropertyTypeDispatcher<String, Representation>
{
    @Override
    protected Representation dispatchBooleanProperty( boolean property, String param )
    {
        return bool( property );
    }

    @Override
    protected Representation dispatchDoubleProperty( double property, String param )
    {
        return number( property );
    }

    @Override
    protected Representation dispatchFloatProperty( float property, String param )
    {
        return number( property );
    }

    @Override
    protected Representation dispatchIntegerProperty( int property, String param )
    {
        return number( property );
    }

    @Override
    protected Representation dispatchLongProperty( long property, String param )
    {
        return number( property );
    }

    @Override
    protected Representation dispatchShortProperty( short property, String param )
    {
        return number( property );
    }

    @Override
    protected Representation dispatchStringProperty( String property, String param )
    {
        return string( property );
    }

    @Override
    protected Representation dispatchStringArrayProperty( String[] array, String param )
    {
        ArrayList<Representation> values = new ArrayList<>();
        for ( String z : array )
        {
            values.add( string( z ) );
        }
        return new ListRepresentation( "", values );
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchShortArrayProperty( PropertyArray<short[], Short> array, String param )
    {
        ArrayList<Representation> values = new ArrayList<>();
        for ( Short z : array )
        {
            values.add( number( z ) );
        }
        return new ListRepresentation( "", values );
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchIntegerArrayProperty( PropertyArray<int[], Integer> array, String param )
    {
        ArrayList<Representation> values = new ArrayList<>();
        for ( Integer z : array )
        {
            values.add( number( z ) );
        }
        return new ListRepresentation( "", values );
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchLongArrayProperty( PropertyArray<long[], Long> array, String param )
    {
        ArrayList<Representation> values = new ArrayList<>();
        for ( Long z : array )
        {
            values.add( number( z ) );
        }
        return new ListRepresentation( "", values );
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchFloatArrayProperty( PropertyArray<float[], Float> array, String param )
    {

        ArrayList<Representation> values = new ArrayList<>();
        for ( Float z : array )
        {
            values.add( number( z ) );
        }
        return new ListRepresentation( "", values );
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchDoubleArrayProperty( PropertyArray<double[], Double> array, String param )
    {
        ArrayList<Representation> values = new ArrayList<>();
        for ( Double z : array )
        {
            values.add( number( z ) );
        }
        return new ListRepresentation( "", values );
    }

    @Override
    @SuppressWarnings( "boxing" )
    protected Representation dispatchBooleanArrayProperty( PropertyArray<boolean[], Boolean> array, String param )
    {
        ArrayList<Representation> values = new ArrayList<>();
        for ( Boolean z : array )
        {
            values.add( bool( z ) );
        }
        return new ListRepresentation( "", values );
    }

    @Override
    protected Representation dispatchByteProperty( byte property, String param )
    {
        throw new UnsupportedOperationException( "Representing bytes not implemented." );
    }

    @Override
    protected Representation dispatchCharacterProperty( char property, String param )
    {
        return number( property );
    }
}
