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
package org.neo4j.kernel.impl.coreapi;

import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;

/**
 * Facade exposing auto indexing operations for nodes.
 */
public class AutoIndexerFacade<T extends PropertyContainer> implements org.neo4j.graphdb.index.AutoIndexer<T>
{
    private final Supplier<ReadableIndex<T>> indexProvider;
    private final AutoIndexOperations autoIndexing;

    public AutoIndexerFacade( Supplier<ReadableIndex<T>> indexProvider, AutoIndexOperations autoIndexing )
    {
        this.indexProvider = indexProvider;
        this.autoIndexing = autoIndexing;
    }

    @Override
    public void setEnabled( boolean enabled )
    {
        autoIndexing.enabled( enabled );
    }

    @Override
    public boolean isEnabled()
    {
        return autoIndexing.enabled();
    }

    @Override
    public ReadableIndex<T> getAutoIndex()
    {
        return indexProvider.get();
    }

    @Override
    public void startAutoIndexingProperty( String propName )
    {
        autoIndexing.startAutoIndexingProperty( propName );
    }

    @Override
    public void stopAutoIndexingProperty( String propName )
    {
        autoIndexing.stopAutoIndexingProperty( propName );
    }

    @Override
    public Set<String> getAutoIndexedProperties()
    {
        return autoIndexing.getAutoIndexedProperties();
    }
}
