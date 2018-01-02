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
package org.neo4j.server.rest.management.repr;

import java.util.ArrayList;

import javax.management.openmbean.CompositeData;

import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationDispatcher;
import org.neo4j.server.rest.repr.ValueRepresentation;

/**
 * Converts CompositeData, to allow representations of JMX beans.
 */
public class JmxAttributeRepresentationDispatcher extends RepresentationDispatcher
{
    @Override
    protected Representation dispatchOtherProperty( Object property, String param )
    {
        if ( property instanceof CompositeData )
        {
            return dispatchCompositeData( (CompositeData) property );
        }
        else
        {
            return ValueRepresentation.string( property.toString() );
        }
    }

    @Override
    protected Representation dispatchOtherArray( Object[] property, String param )
    {
        if ( property instanceof CompositeData[] )
        {
            return dispatchCompositeDataArray( (CompositeData[]) property, param );
        }
        else
        {
            return super.dispatchOtherArray( property, param );
        }
    }

    private JmxCompositeDataRepresentation dispatchCompositeData( CompositeData property )
    {
        return new JmxCompositeDataRepresentation( property );
    }

    private Representation dispatchCompositeDataArray( CompositeData[] property, String param )
    {
        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( CompositeData value : property )
        {
            values.add( new JmxCompositeDataRepresentation( value ) );
        }
        return new ListRepresentation( "", values );
    }

}
