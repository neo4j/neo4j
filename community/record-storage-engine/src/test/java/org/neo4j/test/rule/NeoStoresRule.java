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

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
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
    private JobScheduler jobScheduler;

    private final StoreType[] stores;
    private DefaultIdGeneratorFactory idGeneratorFactory;

    public NeoStoresRule( Class<?> testClass, StoreType... stores )
    {
        this.testClass = testClass;
        this.stores = stores;
    }

    public Builder builder()
    {
        return new Builder();
    }

    private NeoStores open( FileSystemAbstraction fs, PageCache pageCache, RecordFormats format, String... config ) throws IOException
    {
        assert neoStores == null : "Already opened";
        TestDirectory testDirectory = TestDirectory.testDirectory( fs );
        testDirectory.prepareDirectory( testClass, null );
        Config configuration = configOf( config );
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        StoreFactory storeFactory = new StoreFactory( testDirectory.databaseLayout(), configuration, idGeneratorFactory,
                pageCache, fs, format, NullLogProvider.getInstance() );
        return neoStores = stores.length == 0
                ? storeFactory.openAllNeoStores( true )
                : storeFactory.openNeoStores( true, stores );
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }

    private static Config configOf( String... config )
    {
        return Config.newBuilder().setRaw( stringMap( config ) ).build();
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        IOUtils.closeAll( neoStores, rulePageCache, jobScheduler );
        neoStores = null;
        if ( ruleFs != null )
        {
            ruleFs.close();
        }
    }

    public class Builder
    {
        private FileSystemAbstraction fs;
        private String[] config;
        private RecordFormats format;
        private PageCache pageCache;

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
                jobScheduler = new ThreadPoolJobScheduler();
                pageCache = rulePageCache( dbConfig, fs, jobScheduler );
            }
            if ( format == null )
            {
                format = RecordFormatSelector.selectForConfig( dbConfig, NullLogProvider.getInstance() );
            }
            return open( fs, pageCache, format, config );
        }
    }

    private PageCache rulePageCache( Config dbConfig, FileSystemAbstraction fs, JobScheduler scheduler )
    {
        return rulePageCache = getOrCreatePageCache( dbConfig, fs, scheduler );
    }

    private EphemeralFileSystemAbstraction ruleFs()
    {
        return ruleFs = new EphemeralFileSystemAbstraction();
    }

    private static PageCache getOrCreatePageCache( Config config, FileSystemAbstraction fs, JobScheduler jobScheduler )
    {
        SingleFilePageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.open( fs );
        return new MuninnPageCache( swapperFactory, 1000, NULL, PageCursorTracerSupplier.NULL, EmptyVersionContextSupplier.EMPTY, jobScheduler );
    }
}
