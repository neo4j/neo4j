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
package org.neo4j.kernel.internal;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;

public final class DefaultKernelData extends KernelData implements Lifecycle
{
    private final GraphDatabaseAPI graphDb;

    public DefaultKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config,
                              GraphDatabaseAPI graphDb )
    {
        super( fileSystem, pageCache, storeDir, config );
        this.graphDb = graphDb;
    }

    @Override
    public Version version()
    {
        return Version.getKernel();
    }

    @Override
    public GraphDatabaseAPI graphDatabase()
    {
        return graphDb;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }
}
