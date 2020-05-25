/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.security.systemgraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.graphdb.Transaction;

public class KnownSystemComponentVersions<T extends KnownSystemComponentVersion>
{
    public final T noSecurityGraph;
    private final ArrayList<T> knownComponentVersions = new ArrayList<>();

    public KnownSystemComponentVersions( T noSecurityGraph )
    {
        this.noSecurityGraph = noSecurityGraph;
    }

    public void add( T version )
    {
        knownComponentVersions.add( version );
    }

    public T detectCurrentSecurityGraphVersion( Transaction tx )
    {
        List<T> sortedVersions = new ArrayList<>( knownComponentVersions );
        sortedVersions.sort( Comparator.comparingInt( v -> v.version ) );
        Collections.reverse( sortedVersions );    // Sort from most recent to oldest
        for ( T version : sortedVersions )
        {
            if ( version.detected( tx ) )
            {
                return version;
            }
        }
        return noSecurityGraph;
    }

    public T latestSecurityGraphVersion()
    {
        T latest = noSecurityGraph;
        for ( T version : knownComponentVersions )
        {
            if ( version.version > latest.version )
            {
                latest = version;
            }
        }
        return latest;
    }

    public T findSecurityGraphVersion( String substring )
    {
        for ( T version : knownComponentVersions )
        {
            if ( version.description.equals( substring ) )
            {
                return version;
            }
        }
        return noSecurityGraph;
    }
}
