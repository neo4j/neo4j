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

package org.neo4j.server.webadmin.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.management.openmbean.CompositeData;

import org.neo4j.server.rest.domain.Representation;

public class JmxCompositeDataRepresentation implements Representation
{

    protected CompositeData data;

    public JmxCompositeDataRepresentation( CompositeData data )
    {
        this.data = data;
    }

    public Object serialize()
    {
        Map<String, Object> serialData = new HashMap<String, Object>();

        serialData.put( "type", data.getCompositeType().getTypeName() );
        serialData.put( "description", data.getCompositeType().getDescription() );

        ArrayList<Object> values = new ArrayList<Object>();
        for ( Object key : data.getCompositeType().keySet() )
        {
            Map<String, Object> value = new HashMap<String, Object>();
            value.put( "name", key );
            value.put( "description", data.getCompositeType().getDescription(
                    (String) key ) );

            Object rawValue = data.get( (String) key );
            if ( rawValue instanceof CompositeData )
            {
                value.put( "value", ( new JmxCompositeDataRepresentation(
                        (CompositeData) rawValue ) ).serialize() );
            }
            else
            {
                value.put( "value", rawValue.toString() );
            }

            values.add( value );
        }

        serialData.put( "value", values );

        return serialData;
    }
}
