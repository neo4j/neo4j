/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;

public class DefaultSchemaIndexProviderMap implements SchemaIndexProviderMap
{
    private final SchemaIndexProvider defaultIndexProvider;
    private final Map<SchemaIndexProvider.Descriptor,SchemaIndexProvider> indexProviders = new HashMap<>();

    public DefaultSchemaIndexProviderMap( SchemaIndexProvider defaultIndexProvider )
    {
        this( defaultIndexProvider, Collections.emptyList() );
    }

    public DefaultSchemaIndexProviderMap( SchemaIndexProvider defaultIndexProvider,
            Iterable<SchemaIndexProvider> additionalIndexProviders )
    {
        this.defaultIndexProvider = defaultIndexProvider;
        indexProviders.put( defaultIndexProvider.getProviderDescriptor(), defaultIndexProvider );
        for ( SchemaIndexProvider provider : additionalIndexProviders )
        {
            Descriptor providerDescriptor = provider.getProviderDescriptor();
            Objects.requireNonNull( providerDescriptor );
            SchemaIndexProvider existing = indexProviders.putIfAbsent( providerDescriptor, provider );
            if ( existing != null )
            {
                throw new IllegalArgumentException( "Tried to load multiple schema index providers with the same provider descriptor " +
                        providerDescriptor + ". First loaded " + existing + " then " + provider );
            }
        }
    }

    @Override
    public SchemaIndexProvider getDefaultProvider()
    {
        return defaultIndexProvider;
    }

    @Override
    public SchemaIndexProvider apply( SchemaIndexProvider.Descriptor descriptor )
    {
        SchemaIndexProvider provider = indexProviders.get( descriptor );
        if ( provider != null )
        {
            return provider;
        }

        throw new IllegalArgumentException( "Tried to get index provider for an existing index with provider " +
                descriptor + " whereas available providers in this session being " + indexProviders +
                ", and default being " + defaultIndexProvider );
    }

    @Override
    public void accept( Consumer<SchemaIndexProvider> visitor )
    {
        indexProviders.values().forEach( visitor );
    }
}
