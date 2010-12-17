/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.rest.representations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.management.openmbean.CompositeData;

import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.ObjectRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ValueRepresentation;

public class JmxCompositeDataRepresentation extends ObjectRepresentation
{
    protected CompositeData data;

    public JmxCompositeDataRepresentation( CompositeData data )
    {
        super( "jmxCompositeData" );
        this.data = data;
    }

    @Mapping( "type" )
    public ValueRepresentation getType()
    {
        return ValueRepresentation.string( data.getCompositeType().getTypeName() );
    }


    @Mapping( "description" )
    public ValueRepresentation getDescription()
    {
        return ValueRepresentation.string( data.getCompositeType().getDescription() );
    }

    @Mapping( "value" )
    public ListRepresentation getValue()
    {
        Map<String, Object> serialData = new HashMap<String, Object>();

        serialData.put( "type", data.getCompositeType().getTypeName() );
        serialData.put( "description", data.getCompositeType().getDescription() );

        ArrayList<Representation> values = new ArrayList<Representation>();
        for ( Object key : data.getCompositeType().keySet() )
        {
            String name = key.toString();
            String description = data.getCompositeType().getDescription( name );
            Representation value = representationify( data.get( name ) );

            values.add( new NameDescriptionValueRepresentation( name, description, value ) );
        }

        return new ListRepresentation( "value", values );
    }

    private Representation representationify( Object rawValue )
    {
        if ( rawValue instanceof CompositeData )
        {
            return new JmxCompositeDataRepresentation( (CompositeData)rawValue );
        } else
        {
            return ValueRepresentation.string( rawValue.toString() );
        }
    }
}
