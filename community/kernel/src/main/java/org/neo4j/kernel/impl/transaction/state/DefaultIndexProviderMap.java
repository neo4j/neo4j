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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DefaultIndexProviderMap extends LifecycleAdapter implements IndexProviderMap
{
    private final Map<IndexProviderDescriptor,IndexProvider> indexProvidersByDescriptor = new HashMap<>();
    private final Map<String,IndexProvider> indexProvidersByName = new HashMap<>();
    private final DependencyResolver dependencies;
    private IndexProvider defaultIndexProvider;
    private final Config config;

    public DefaultIndexProviderMap( DependencyResolver dependencies, Config config )
    {
        this.dependencies = dependencies;
        this.config = config;
    }

    @Override
    public void init()
    {
        Iterable<? extends IndexProvider> indexProviders = dependencies.resolveTypeDependencies( IndexProvider.class );
        for ( IndexProvider provider : indexProviders )
        {
            IndexProviderDescriptor providerDescriptor = provider.getProviderDescriptor();
            requireNonNull( providerDescriptor );
            IndexProvider existing = put( providerDescriptor, provider );
            if ( existing != null )
            {
                throw new IllegalArgumentException( "Tried to load multiple schema index providers with the same provider descriptor " +
                        providerDescriptor + ". First loaded " + existing + " then " + provider );
            }
        }
        initDefaultProvider();
    }

    @Override
    public IndexProvider getDefaultProvider()
    {
        assertInit();
        return defaultIndexProvider;
    }

    @Override
    public IndexProvider lookup( IndexProviderDescriptor providerDescriptor )
    {
        assertInit();
        IndexProvider provider = indexProvidersByDescriptor.get( providerDescriptor );
        assertProviderFound( provider, providerDescriptor.name() );
        return provider;
    }

    @Override
    public IndexProvider lookup( String providerDescriptorName ) throws IndexProviderNotFoundException
    {
        assertInit();
        IndexProvider provider = indexProvidersByName.get( providerDescriptorName );
        assertProviderFound( provider, providerDescriptorName );
        return provider;
    }

    @Override
    public void accept( Consumer<IndexProvider> visitor )
    {
        assertInit();
        indexProvidersByDescriptor.values().forEach( visitor );
    }

    private void assertProviderFound( IndexProvider provider, String providerDescriptorName )
    {
        if ( provider == null )
        {
            throw new IndexProviderNotFoundException( "Tried to get index provider with name " + providerDescriptorName +
                    " whereas available providers in this session being " + indexProvidersByName.keySet() + ", and default being " +
                    defaultIndexProvider.getProviderDescriptor().name() );
        }
    }

    private void assertInit()
    {
        if ( defaultIndexProvider == null )
        {
            throw new IllegalStateException( "DefaultIndexProviderMap must be part of life cycle and initialized before getting providers." );
        }
    }

    private void initDefaultProvider()
    {
        String providerName = config.get( GraphDatabaseSettings.default_schema_provider );
        IndexProvider configuredDefaultProvider = indexProvidersByName.get( providerName );
        requireNonNull( configuredDefaultProvider, () -> format( "Configured default provider: `%s` not found. Available index providers: %s.", providerName,
                indexProvidersByName.keySet().toString() ) );
        defaultIndexProvider = configuredDefaultProvider;
    }

    private IndexProvider put( IndexProviderDescriptor providerDescriptor, IndexProvider provider )
    {
        IndexProvider existing = indexProvidersByDescriptor.putIfAbsent( providerDescriptor, provider );
        indexProvidersByName.putIfAbsent( providerDescriptor.name(), provider );
        return existing;
    }
}
