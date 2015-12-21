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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.io.File;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.ExternalResource;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.impl.EphemeralIdGenerator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Conveniently manages a {@link RecordStorageEngine} in a test. Needs {@link FileSystemAbstraction} and
 * {@link PageCache}, which usually are managed by test rules themselves. That's why they are passed in
 * when {@link #getWith(FileSystemAbstraction, PageCache) getting (constructing)} the engine. Further
 * dependencies can be overridden in that returned builder as well.
 *
 * Keep in mind that this rule must be created BEFORE {@link PageCacheRule} and any file system rule so that
 * shutdown order gets correct.
 */
public class RecordStorageEngineRule extends ExternalResource
{
    private final LifeSupport life = new LifeSupport();

    @Override
    protected void before() throws Throwable
    {
        super.before();
        life.start();
    }

    public Builder getWith( FileSystemAbstraction fs, PageCache pageCache )
    {
        return new Builder( fs, pageCache );
    }

    private RecordStorageEngine get( FileSystemAbstraction fs, PageCache pageCache, LabelScanStore labelScanStore,
            SchemaIndexProvider schemaIndexProvider, DatabaseHealth databaseHealth, File storeDirectory )
    {
        if ( !fs.fileExists( storeDirectory ) && !fs.mkdir( storeDirectory ) )
        {
            throw new IllegalStateException();
        }
        IdGeneratorFactory idGeneratorFactory = new EphemeralIdGenerator.Factory();
        LabelScanStoreProvider labelScanStoreProvider = new LabelScanStoreProvider( labelScanStore, 42 );
        LegacyIndexProviderLookup legacyIndexProviderLookup = mock( LegacyIndexProviderLookup.class );
        when( legacyIndexProviderLookup.all() ).thenReturn( Iterables.empty() );
        IndexConfigStore indexConfigStore = new IndexConfigStore( storeDirectory, fs );
        JobScheduler scheduler = life.add( new Neo4jJobScheduler() );

        return life.add( new RecordStorageEngine( storeDirectory, new Config(), idGeneratorFactory, pageCache, fs,
                NullLogProvider.getInstance(), mock( PropertyKeyTokenHolder.class ), mock( LabelTokenHolder.class ),
                mock( RelationshipTypeTokenHolder.class ), () -> {}, new StandardConstraintSemantics(),
                scheduler, mock( TokenNameLookup.class ), new ReentrantLockService(),
                schemaIndexProvider, IndexingService.NO_MONITOR, databaseHealth,
                labelScanStoreProvider, legacyIndexProviderLookup, indexConfigStore ) );
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        life.shutdown();
        super.after( successful );
    }

    public class Builder
    {
        private final FileSystemAbstraction fs;
        private final PageCache pageCache;
        private LabelScanStore labelScanStore = new InMemoryLabelScanStore();
        private DatabaseHealth databaseHealth = new DatabaseHealth(
                new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) ),
                NullLog.getInstance() );
        private File storeDirectory = new File( "graph.db" );
        private SchemaIndexProvider schemaIndexProvider = SchemaIndexProvider.NO_INDEX_PROVIDER;

        public Builder( FileSystemAbstraction fs, PageCache pageCache )
        {
            this.fs = fs;
            this.pageCache = pageCache;
        }

        public Builder labelScanStore( LabelScanStore labelScanStore )
        {
            this.labelScanStore = labelScanStore;
            return this;
        }

        public Builder indexProvider( SchemaIndexProvider schemaIndexProvider )
        {
            this.schemaIndexProvider = schemaIndexProvider;
            return this;
        }

        public Builder databaseHealth( DatabaseHealth databaseHealth )
        {
            this.databaseHealth = databaseHealth;
            return this;
        }

        public Builder storeDirectory( File storeDirectory )
        {
            this.storeDirectory = storeDirectory;
            return this;
        }

        // Add more here

        public RecordStorageEngine build()
        {
            return get( fs, pageCache, labelScanStore, schemaIndexProvider, databaseHealth, storeDirectory );
        }
    }
}
