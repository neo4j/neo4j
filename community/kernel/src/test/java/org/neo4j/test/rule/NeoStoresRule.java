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
package org.neo4j.test.rule;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
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
 * Rule for opening a {@link NeoStores}.
 */
public class NeoStoresRule extends ExternalResource
{
    private final Class<?> testClass;
    private NeoStores neoStores;

    // Custom components which are managed by this rule if user doesn't supply them
    private EphemeralFileSystemAbstraction ruleFs;
    private PageCache rulePageCache;

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

    private NeoStores open( FileSystemAbstraction fs, PageCache pageCache, RecordFormats format,
            Function<FileSystemAbstraction,IdGeneratorFactory> idGeneratorFactory, String... config )
                    throws IOException
    {
        assert neoStores == null : "Already opened";
        TestDirectory testDirectory = TestDirectory.testDirectory( testClass, fs );
        File storeDir = testDirectory.makeGraphDbDir();
        Config configuration = configOf( config );
        StoreFactory storeFactory = new StoreFactory( storeDir, configuration, idGeneratorFactory.apply( fs ),
                pageCache, fs, format, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        return neoStores = stores.length == 0
                ? storeFactory.openAllNeoStores( true )
                : storeFactory.openNeoStores( true, stores );
    }

    private static Config configOf( String... config )
    {
        return Config.defaults( stringMap( config ) );
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        IOUtils.closeAll( neoStores, rulePageCache );
        neoStores = null;
        if ( ruleFs != null )
        {
            ruleFs.close();
        }
    }

    private static PageCache getOrCreatePageCache( Config config, FileSystemAbstraction fs )
    {
        Log log = NullLog.getInstance();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( fs, config, NULL,
                PageCursorTracerSupplier.NULL, log, EmptyVersionContextSupplier.EMPTY );
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
            if ( fs == null )
            {
                fs = ruleFs();
            }
            if ( config == null )
            {
                config = new String[0];
            }
            Config dbConfig = configOf( config );
            if ( pageCache == null )
            {
                pageCache = rulePageCache( dbConfig, fs );
            }
            if ( format == null )
            {
                format = RecordFormatSelector.selectForConfig( dbConfig, NullLogProvider.getInstance() );
            }
            if ( idGeneratorFactory == null )
            {
                idGeneratorFactory = DefaultIdGeneratorFactory::new;
            }
            return open( fs, pageCache, format, idGeneratorFactory, config );
        }
    }

    private PageCache rulePageCache( Config dbConfig, FileSystemAbstraction fs )
    {
        return rulePageCache = getOrCreatePageCache( dbConfig, fs );
    }

    private EphemeralFileSystemAbstraction ruleFs()
    {
        return ruleFs = new EphemeralFileSystemAbstraction();
    }
}
