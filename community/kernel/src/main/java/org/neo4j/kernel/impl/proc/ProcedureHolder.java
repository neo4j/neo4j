/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.proc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
    private final Map<QualifiedName,Integer> caseInsensitveName2Id = new HashMap<>();
    private final ArrayList<T> store = new ArrayList<>();

    T get( QualifiedName name )
    {
        Integer id = name2Id( name );
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

    void put( QualifiedName name, T item, boolean caseInsensitive )
    {
        int id = store.size();
        store.add( item );
        nameToId.put( name, id );
        if ( caseInsensitive )
        {
            caseInsensitveName2Id.put( toLowerCaseName( name ), id );
        }
        else
        {
            caseInsensitveName2Id.remove( toLowerCaseName( name ) );
        }
    }

    int idOf( QualifiedName name )
    {
        Integer id = name2Id( name );
        if ( id == null )
        {
            throw new NoSuchElementException();
        }

        return id;
    }

    List<T> all()
    {
        return Collections.unmodifiableList( store );
    }

    private Integer name2Id( QualifiedName name )
    {
        Integer id = nameToId.get( name );
        if ( id == null )
        { // Did not find it in the case sensitive lookup - let's check for case insensitive objects
            QualifiedName lowerCaseName = toLowerCaseName( name );
            id = caseInsensitveName2Id.get( lowerCaseName );
        }
        return id;
    }

    private QualifiedName toLowerCaseName( QualifiedName name )
    {
        String[] oldNs = name.namespace();
        String[] lowerCaseNamespace = new String[oldNs.length];
        for ( int i = 0; i < oldNs.length; i++ )
        {
            lowerCaseNamespace[i] = oldNs[i].toLowerCase();
        }
        String lowercaseName = name.name().toLowerCase();
        return new QualifiedName( lowerCaseNamespace, lowercaseName );
    }
}
