/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.test;

import java.io.File;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

/**
 * Rule for opening a {@link NeoStores}, either via {@link #open(String...)}, which just uses an in-memory
 * file system, or via {@link #open(FileSystemAbstraction, PageCache, String...)} which is suitable in an
 * environment where you already have an fs and page cache available.
 */
public class NeoStoresRule extends ExternalResource
{
    private final Class<?> testClass;
    private TargetDirectory targetDirectory;
    private NeoStores neoStores;
    private EphemeralFileSystemAbstraction efs;
    private PageCache pageCache;
    private StoreFactory storeFactory;

    public NeoStoresRule( Class<?> testClass )
    {
        this.testClass = testClass;
    }

    public NeoStores open( String... config )
    {
        efs = new EphemeralFileSystemAbstraction();
        Config conf = new Config( stringMap( config ) );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( efs, conf, NULL,
                NullLog.getInstance() );
        pageCache = pageCacheFactory.getOrCreatePageCache();
        return open( efs, pageCache, config );
    }

    public NeoStores open( FileSystemAbstraction fs, PageCache pageCache, String... config )
    {
        assert neoStores == null : "Already opened";
        TargetDirectory targetDirectory = new TargetDirectory( fs, testClass );
        File storeDir = targetDirectory.makeGraphDbDir();
        Config configuration = new Config( stringMap( config ) );
        storeFactory = new StoreFactory( storeDir, configuration, new DefaultIdGeneratorFactory( fs ),
                pageCache, fs, NullLogProvider.getInstance() );
        return neoStores = storeFactory.openAllNeoStores( true );
    }

    public NeoStores reopen() {
        return storeFactory.openAllNeoStores();
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
        if ( pageCache != null )
        {
            pageCache.close();
        }
        if ( targetDirectory != null )
        {
            targetDirectory.cleanup();
        }
        if ( efs != null )
        {
            efs.shutdown();
        }
    }
}
