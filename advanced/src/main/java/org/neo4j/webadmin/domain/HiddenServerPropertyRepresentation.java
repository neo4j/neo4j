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

package org.neo4j.webadmin.domain;

import java.util.Map;

import org.neo4j.webadmin.properties.ValueDefinition;

/**
 * A specific type of server property that is defined and fancy, is exposed via
 * the REST configuration API and is changeable just like its parent.
 * 
 * The only difference is that settings defined with this class do not show up
 * in the admin configuration UI.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class HiddenServerPropertyRepresentation extends
        ServerPropertyRepresentation
{

    public HiddenServerPropertyRepresentation( String key, String value,
            PropertyType type )
    {
        super( key, value, type );
    }

    public HiddenServerPropertyRepresentation( String key, String displayName,
            String value, PropertyType type )
    {
        super( key, displayName, value, type );
    }

    public HiddenServerPropertyRepresentation( String key, String displayName,
            String value, PropertyType type, ValueDefinition valueDefinition )
    {
        super( key, displayName, value, type, valueDefinition );
    }

    public Object serialize()
    {
        @SuppressWarnings( "unchecked" ) Map<String, Object> map = (Map<String, Object>) super.serialize();
        map.put( "hidden", true );
        return map;
    }

}
