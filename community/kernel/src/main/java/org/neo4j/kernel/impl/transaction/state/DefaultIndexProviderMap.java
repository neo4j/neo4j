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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProvider.Descriptor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;

public class DefaultIndexProviderMap implements IndexProviderMap
{
    private final IndexProvider defaultIndexProvider;
    private final Map<IndexProvider.Descriptor,IndexProvider> indexProviders = new HashMap<>();
    private final Map<String,IndexProvider> indexProvidersByName = new HashMap<>();

    public DefaultIndexProviderMap( IndexProvider defaultIndexProvider )
    {
        this( defaultIndexProvider, Collections.emptyList() );
    }

    public DefaultIndexProviderMap( IndexProvider defaultIndexProvider,
                                    Iterable<IndexProvider> additionalIndexProviders )
    {
        this.defaultIndexProvider = defaultIndexProvider;
        put( defaultIndexProvider.getProviderDescriptor(), defaultIndexProvider );
        for ( IndexProvider provider : additionalIndexProviders )
        {
            Descriptor providerDescriptor = provider.getProviderDescriptor();
            Objects.requireNonNull( providerDescriptor );
            IndexProvider existing = put( providerDescriptor, provider );
            if ( existing != null )
            {
                throw new IllegalArgumentException( "Tried to load multiple schema index providers with the same provider descriptor " +
                        providerDescriptor + ". First loaded " + existing + " then " + provider );
            }
        }
    }

    private IndexProvider put( Descriptor providerDescriptor, IndexProvider provider )
    {
        IndexProvider existing = indexProviders.putIfAbsent( providerDescriptor, provider );
        indexProvidersByName.put( providerDescriptor.name(), provider );
        return existing;
    }

    @Override
    public IndexProvider getDefaultProvider()
    {
        return defaultIndexProvider;
    }

    @Override
    public IndexProvider lookup( IndexProvider.Descriptor providerDescriptor )
    {
        IndexProvider provider = indexProviders.get( providerDescriptor );
        if ( provider != null )
        {
            return provider;
        }

        throw notFound( providerDescriptor );
    }

    @Override
    public IndexProvider lookup( String providerDescriptorName ) throws IndexProviderNotFoundException
    {
        IndexProvider provider = indexProvidersByName.get( providerDescriptorName );
        if ( provider != null )
        {
            return provider;
        }

        throw notFound( providerDescriptorName );
    }

    private IllegalArgumentException notFound( Object key )
    {
        return new IllegalArgumentException( "Tried to get index provider with name " + key +
                " whereas available providers in this session being " + Arrays.toString( indexProviderNames() ) + ", and default being " +
                defaultIndexProvider.getProviderDescriptor().name() );
    }

    @Override
    public void accept( Consumer<IndexProvider> visitor )
    {
        indexProviders.values().forEach( visitor );
    }

    private String[] indexProviderNames()
    {
        Collection<IndexProvider> providerList = indexProviders.values();
        String[] providerNames = new String[providerList.size()];
        int index = 0;
        for ( IndexProvider indexProvider : providerList )
        {
            providerNames[index++] = indexProvider.getProviderDescriptor().name();
        }
        return providerNames;
    }
}
