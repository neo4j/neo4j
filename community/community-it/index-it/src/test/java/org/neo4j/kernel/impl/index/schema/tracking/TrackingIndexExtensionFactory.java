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
package org.neo4j.kernel.impl.index.schema.tracking;

import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;

public class TrackingIndexExtensionFactory extends ExtensionFactory<TrackingIndexExtensionFactory.Dependencies>
{
    private final ConcurrentHashMap<String,TrackingReadersIndexProvider> indexProvider = new ConcurrentHashMap<>();
    private final AbstractIndexProviderFactory delegate;

    public TrackingIndexExtensionFactory( AbstractIndexProviderFactory delegate )
    {
        super( ExtensionType.DATABASE, "trackingIndex" );
        this.delegate = delegate;
    }

    public interface Dependencies extends AbstractIndexProviderFactory.Dependencies
    {
        Database database();
    }

    @Override
    public synchronized IndexProvider newInstance( ExtensionContext context, Dependencies dependencies )
    {
        NamedDatabaseId namedDatabaseId = dependencies.database().getNamedDatabaseId();
        return indexProvider.computeIfAbsent( namedDatabaseId.name(), s ->
        {
            IndexProvider indexProvider = delegate.newInstance( context, dependencies );
            return new TrackingReadersIndexProvider( indexProvider );
        } );
    }

    public TrackingReadersIndexProvider getIndexProvider( String databaseName )
    {
        return indexProvider.get( databaseName );
    }
}
