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
package org.neo4j.test.rule;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

/**
 * Rule for opening a {@link NeoStores}, either via {@link #open(String...)}, which just uses an in-memory
 * file system, or via {@link #open(FileSystemAbstraction, PageCache, RecordFormats, String...)} which is suitable in an
 * environment where you already have an fs and page cache available.
 */
public class NeoStoresRule extends ExternalResource
{
    private final Class<?> testClass;
    private NeoStores neoStores;
    private EphemeralFileSystemAbstraction efs;
    private PageCache pageCache;
    private final StoreType[] stores;

    public NeoStoresRule( Class<?> testClass, StoreType... stores )
    {
        this.testClass = testClass;
        this.stores = stores;
    }

    public Builder builder()
    {
        return new Builder();
    }

    public NeoStores open( String... config ) throws IOException
    {
        Config configuration = Config.embeddedDefaults( stringMap( config ) );
        RecordFormats formats = RecordFormatSelector.selectForConfig( configuration, NullLogProvider.getInstance() );
        return open( formats, config );
    }

    public NeoStores open( RecordFormats format, String... config ) throws IOException
    {
        efs = new EphemeralFileSystemAbstraction();
        Config conf = Config.embeddedDefaults( stringMap( config ) );
        pageCache = getOrCreatePageCache( conf, efs );
        return open( efs, pageCache, format, fs -> new DefaultIdGeneratorFactory( fs ), config );
    }

    public NeoStores open( FileSystemAbstraction fs, PageCache pageCache, RecordFormats format,
            Function<FileSystemAbstraction,IdGeneratorFactory> idGeneratorFactory, String... config )
                    throws IOException
    {
        assert neoStores == null : "Already opened";
        if ( fs == null )
        {
            fs = efs = new EphemeralFileSystemAbstraction();
        }
        TestDirectory testDirectory = TestDirectory.testDirectory( testClass, fs );
        File storeDir = testDirectory.makeGraphDbDir();
        if ( config == null )
        {
            config = new String[0];
        }
        Config configuration = Config.embeddedDefaults( stringMap( config ) );
        if ( pageCache == null )
        {
            pageCache = this.pageCache = getOrCreatePageCache( configuration, fs );
        }
        if ( format == null )
        {
            format = RecordFormatSelector.defaultFormat();
        }
        StoreFactory storeFactory = new StoreFactory( storeDir, configuration, idGeneratorFactory.apply( fs ),
                pageCache, fs, format, NullLogProvider.getInstance() );
        return neoStores = stores.length == 0
                ? storeFactory.openAllNeoStores( true )
                : storeFactory.openNeoStores( true, stores );
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        IOUtils.closeAll( neoStores, pageCache, efs );
    }

    private static PageCache getOrCreatePageCache( Config config, FileSystemAbstraction fs )
    {
        Log log = NullLog.getInstance();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( fs, config, NULL,
                PageCursorTracerSupplier.NULL, log );
        return pageCacheFactory.getOrCreatePageCache();
    }

    public class Builder
    {
        private FileSystemAbstraction fs;
        private String[] config;
        private RecordFormats format;
        private PageCache pageCache;
        private Function<FileSystemAbstraction,IdGeneratorFactory> idGeneratorFactory;

        public Builder with( FileSystemAbstraction fs )
        {
            this.fs = fs;
            return this;
        }

        public Builder with( String... config )
        {
            this.config = config;
            return this;
        }

        public Builder with( RecordFormats format )
        {
            this.format = format;
            return this;
        }

        public Builder with( PageCache pageCache )
        {
            this.pageCache = pageCache;
            return this;
        }

        public Builder with( Function<FileSystemAbstraction,IdGeneratorFactory> idGeneratorFactory )
        {
            this.idGeneratorFactory = idGeneratorFactory;
            return this;
        }

        public NeoStores build() throws IOException
        {
            return open( fs, pageCache, format, idGeneratorFactory, config );
        }
    }
}
