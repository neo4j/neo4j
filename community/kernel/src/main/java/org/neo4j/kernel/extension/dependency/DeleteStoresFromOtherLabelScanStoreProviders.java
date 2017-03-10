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
package org.neo4j.kernel.extension.dependency;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;

/**
 * Given an already selected {@link LabelScanStoreProvider}, tell all the other {@link LabelScanStoreProvider}
 * to {@link LabelScanStoreProvider#drop() drop} their stores, so that there's only one authoritative provider.
 */
public class DeleteStoresFromOtherLabelScanStoreProviders implements DependencyResolver.SelectionStrategy
{
    private final LabelScanStoreProvider providerToKeep;

    public DeleteStoresFromOtherLabelScanStoreProviders( LabelScanStoreProvider providerToKeep )
    {
        this.providerToKeep = providerToKeep;
    }

    @Override
    public <T> T select( Class<T> type, Iterable<T> candidates ) throws IllegalArgumentException
    {
        for ( T candidate : candidates )
        {
            if ( !(candidate instanceof LabelScanStoreProvider) )
            {
                throw new IllegalArgumentException( "May only be used for " + LabelScanStoreProvider.class );
            }

            LabelScanStoreProvider provider = (LabelScanStoreProvider) candidate;
            if ( provider != providerToKeep )
            {
                try
                {
                    if ( !provider.isReadOnly() )
                    {
                        provider.drop();
                    }
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
        }
        return (T) providerToKeep;
    }
}
