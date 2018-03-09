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
package org.neo4j.kernel.impl.proc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.kernel.api.procs.QualifiedName;

/**
 * Simple in memory store for procedures.
 *
 * Should only be accessed from a single thread
 * @param <T> the type to be stored
 */
class ProcedureHolder<T>
{
    private final Map<QualifiedName,Integer> nameToId = new HashMap<>();
    private final ArrayList<T> store = new ArrayList<>();

    T get( QualifiedName name )
    {
        Integer id = nameToId.get( name );
        if ( id == null )
        {
            return null;
        }
        return store.get( id );
    }

    T get( int id )
    {
        return store.get( id );
    }

    void put( QualifiedName name, T item )
    {
        int id = store.size();
        store.add( item );
        nameToId.put( name, id );
    }

    int idOf( QualifiedName name )
    {
        return nameToId.get( name );
    }

    List<T> all()
    {
        return Collections.unmodifiableList( store );
    }
}
