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
package org.neo4j.kernel.impl.api.index;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.kernel.api.index.IndexProvider;

/**
 * Can visit an {@link IndexProviderMap} and extract {@link IndexProvider.Descriptor} matching a given name.
 */
public class IndexProviderDescriptorByName implements Consumer<IndexProvider>, Iterable<IndexProvider.Descriptor>
{
    private final Set<IndexProvider.Descriptor> hits = new HashSet<>();
    private final String name;

    public IndexProviderDescriptorByName( String name )
    {
        this.name = name;
    }

    @Override
    public void accept( IndexProvider indexProvider )
    {
        IndexProvider.Descriptor providerDescriptor = indexProvider.getProviderDescriptor();
        if ( providerDescriptor.name().equals( name ) )
        {
            hits.add( providerDescriptor );
        }
    }

    @Override
    public Iterator<IndexProvider.Descriptor> iterator()
    {
        return hits.iterator();
    }

    public IndexProvider.Descriptor single()
    {
        Iterator<IndexProvider.Descriptor> iterator = hits.iterator();
        if ( !iterator.hasNext() )
        {
            throw new IllegalArgumentException( "No index provider matching name '" + name + "'" );
        }
        IndexProvider.Descriptor single = iterator.next();
        if ( iterator.hasNext() )
        {
            throw new IllegalArgumentException( "Multiple index providers matching name '" + name + "'" );
        }
        return single;
    }
}
