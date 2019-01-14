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
package org.neo4j.kernel.impl.api;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.kernel.spi.explicitindex.IndexProviders;

public class DefaultExplicitIndexProvider implements IndexProviders, ExplicitIndexProvider
{
    private final Map<String,IndexImplementation> indexProviders = new HashMap<>();

    @Override
    public void registerIndexProvider( String name, IndexImplementation index )
    {
        if ( indexProviders.containsKey( name ) )
        {
            throw new IllegalArgumentException( "Index provider '" + name + "' already registered" );
        }
        indexProviders.put( name, index );
    }

    @Override
    public boolean unregisterIndexProvider( String name )
    {
        IndexImplementation removed = indexProviders.remove( name );
        return removed != null;
    }

    @Override
    public IndexImplementation getProviderByName( String name )
    {
        IndexImplementation provider = indexProviders.get( name );
        if ( provider == null )
        {
            throw new IllegalArgumentException( "No index provider '" + name + "' found. Maybe the intended provider (or one more of its " + "dependencies) " +
                    "aren't on the classpath or it failed to load." );
        }
        return provider;
    }

    @Override
    public Iterable<IndexImplementation> allIndexProviders()
    {
        return indexProviders.values();
    }
}
