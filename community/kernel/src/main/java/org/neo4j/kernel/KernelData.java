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
package org.neo4j.kernel;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public abstract class KernelData
{
    public static final Setting<String> forced_id = GraphDatabaseSettings.forced_kernel_id;
    private static final Map<String, KernelData> instances = new ConcurrentHashMap<String, KernelData>();

    private static synchronized String newInstance( KernelData instance )
    {
        String instanceId = instance.configuration.get( forced_id );
        if ( instanceId == null || instanceId.equals( "" ) )
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
                    "There is already a kernel started with " + forced_id.name() + "='" + instanceId + "'." );
        }
        instances.put( instanceId, instance );
        return instanceId;
    }

    private static synchronized void removeInstance( String instanceId )
    {
        if (instances.remove( instanceId ) == null)
            throw new IllegalArgumentException( "No kernel found with instance id "+instanceId );
    }

    private final String instanceId;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final File storeDir;
    private final Config configuration;

    protected KernelData( FileSystemAbstraction fs, PageCache pageCache, File storeDir, Config configuration )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.storeDir = storeDir;
        this.configuration = configuration;
        this.instanceId = newInstance( this );
    }

    public final String instanceId()
    {
        return instanceId;
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

    public abstract Version version();

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

    public abstract GraphDatabaseAPI graphDatabase();

    public void shutdown()
    {
        removeInstance( instanceId );
    }
}
