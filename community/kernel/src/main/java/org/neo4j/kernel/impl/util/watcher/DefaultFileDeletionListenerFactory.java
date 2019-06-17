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
package org.neo4j.kernel.impl.util.watcher;

import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.io.fs.watcher.FileWatchEventListenerFactory;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.logging.internal.LogService;

public class DefaultFileDeletionListenerFactory implements FileWatchEventListenerFactory<DefaultFileDeletionEventListener>
{
    private final NormalizedDatabaseName databaseName;
    private final LogService logService;
    private final Predicate<String> fileNameFilter;

    public DefaultFileDeletionListenerFactory( NormalizedDatabaseName databaseName, LogService logService, Predicate<String> fileNameFilter )
    {
        this.databaseName = databaseName;
        this.logService = logService;
        this.fileNameFilter = fileNameFilter;
    }

    @Override
    public DefaultFileDeletionEventListener createListener( Set<WatchedResource> watchedResources )
    {
        return new DefaultFileDeletionEventListener( databaseName, watchedResources, logService, fileNameFilter );
    }
}
