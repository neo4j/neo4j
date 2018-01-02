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
package org.neo4j.unsafe.impl.batchimport.input;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapping from name to {@link Group}. Assigns proper {@link Group#id() ids} to created groups.
 */
public class Groups
{
    private final Map<String,Group> byName = new HashMap<>();
    private int nextId = 0;
    private Boolean globalMode;

    /**
     * @param name group name or {@code null} for a {@link Group#GLOBAL global group}.
     * @return {@link Group} for the given name. If the group doesn't already exist it will be created
     * with a new id. If {@code name} is {@code null} then the {@link Group#GLOBAL global group} is returned.
     * This method also prevents mixing global and non-global groups, i.e. if first call is {@code null},
     * then consecutive calls have to specify {@code null} name as well. The same holds true for non-null values.
     */
    public Group getOrCreate( String name )
    {
        boolean global = name == null;
        if ( globalMode == null )
        {
            globalMode = global;
        }
        else
        {
            if ( global != globalMode.booleanValue() )
            {
                throw new IllegalStateException( "Mixing specified and unspecified group belongings " +
                        "in a single import isn't supported" );
            }
        }

        if ( name == null )
        {
            return Group.GLOBAL;
        }

        Group group = byName.get( name );
        if ( group == null )
        {
            byName.put( name, group = new Group.Adapter( nextId++, name ) );
        }
        return group;
    }
}
