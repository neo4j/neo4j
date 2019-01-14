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

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.forced_kernel_id;

public class KernelData extends LifecycleAdapter
{
    private static final Map<String, KernelData> instances = new ConcurrentHashMap<>();

    private final String instanceId;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final File storeDir;
    private final Config configuration;
    private final DataSourceManager dataSourceManager;

    public KernelData( FileSystemAbstraction fs, PageCache pageCache, File storeDir, Config configuration, DataSourceManager dataSourceManager )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.storeDir = storeDir;
        this.configuration = configuration;
        this.dataSourceManager = dataSourceManager;
        this.instanceId = newInstance( this );
    }

    public final String instanceId()
    {
        return instanceId;
    }

    public Version version()
    {
        return Version.getKernel();
    }

    public File getStoreDir()
    {
        return storeDir;
    }

    public Config getConfig()
    {
        return configuration;
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public FileSystemAbstraction getFilesystemAbstraction()
    {
        return fs;
    }

    public DataSourceManager getDataSourceManager()
    {
        return dataSourceManager;
    }

    @Override
    public void shutdown()
    {
        removeInstance( instanceId );
    }

    @Override
    public final int hashCode()
    {
        return instanceId.hashCode();
    }

    @Override
    public final boolean equals( Object obj )
    {
        return obj instanceof KernelData && instanceId.equals( ((KernelData) obj).instanceId );
    }

    private static synchronized String newInstance( KernelData instance )
    {
        String instanceId = instance.configuration.get( forced_kernel_id );
        if ( StringUtils.isEmpty( instanceId ) )
        {
            for ( int i = 0; i < instances.size() + 1; i++ )
            {
                instanceId = Integer.toString( i );
                if ( !instances.containsKey( instanceId ) )
                {
                    break;
                }
            }
        }
        if ( instances.containsKey( instanceId ) )
        {
            throw new IllegalStateException(
                    "There is already a kernel started with " + forced_kernel_id.name() + "='" + instanceId + "'." );
        }
        instances.put( instanceId, instance );
        return instanceId;
    }

    private static synchronized void removeInstance( String instanceId )
    {
        if ( instances.remove( instanceId ) == null )
        {
            throw new IllegalArgumentException( "No kernel found with instance id " + instanceId );
        }
    }

}
