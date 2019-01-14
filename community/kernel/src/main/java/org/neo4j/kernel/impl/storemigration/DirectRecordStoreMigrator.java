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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.helpers.ArrayUtil.contains;

/**
 * Idea is to migrate a {@link NeoStores} store by store, record by record in a sequential fashion for
 * quick migration from one {@link RecordFormats} to another.
 */
public class DirectRecordStoreMigrator
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Config config;

    public DirectRecordStoreMigrator( PageCache pageCache, FileSystemAbstraction fs, Config config )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.config = config;
    }

    public void migrate( File fromStoreDir, RecordFormats fromFormat, File toStoreDir, RecordFormats toFormat,
            ProgressReporter progressReporter, StoreType[] types, StoreType... additionalTypesToOpen )
    {
        StoreType[] storesToOpen = ArrayUtil.concat( types, additionalTypesToOpen );
        progressReporter.start( storesToOpen.length );

        try (
                NeoStores fromStores = new StoreFactory( fromStoreDir, config, new DefaultIdGeneratorFactory( fs ),
                    pageCache, fs, fromFormat, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY )
                        .openNeoStores( true, storesToOpen );
                NeoStores toStores = new StoreFactory( toStoreDir, withPersistedStoreHeadersAsConfigFrom( fromStores, storesToOpen ),
                    new DefaultIdGeneratorFactory( fs ), pageCache, fs, toFormat, NullLogProvider.getInstance(),
                        EmptyVersionContextSupplier.EMPTY )
                        .openNeoStores( true, storesToOpen ) )
        {
            for ( StoreType type : types )
            {
                // This condition will exclude counts store first and foremost.
                if ( type.isRecordStore() )
                {
                    migrate( fromStores.getRecordStore( type ), toStores.getRecordStore( type ) );
                    progressReporter.progress( 1 );
                }
            }
        }
    }

    private <RECORD extends AbstractBaseRecord> void migrate( RecordStore<RECORD> from, RecordStore<RECORD> to )
    {
        to.setHighestPossibleIdInUse( from.getHighestPossibleIdInUse() );

        from.scanAllRecords( record ->
        {
            to.prepareForCommit( record );
            to.updateRecord( record );
            return false;
        } );
    }

    /**
     * Creates a configuration to include dynamic record store sizes where data size in old a new format
     * will be the same. This is important because we're doing a record-to-record migration and so
     * data which fits into one record must fit into the other as to not needing additional blocks
     * in the dynamic record chain.
     *
     * @param legacyStores {@link NeoStores} to read dynamic record data sizes from.
     * @param types array of {@link StoreType} which we know that legacy stores have opened.
     * @return a {@link Config} which mimics dynamic record data sizes from the {@code legacyStores}.
     */
    private Config withPersistedStoreHeadersAsConfigFrom( NeoStores legacyStores, StoreType[] types )
    {
        Map<String,String> config = new HashMap<>();
        if ( contains( types, StoreType.RELATIONSHIP_GROUP ) )
        {
            config.put( GraphDatabaseSettings.dense_node_threshold.name(),
                    String.valueOf( legacyStores.getRelationshipGroupStore().getStoreHeaderInt() ) );
        }
        if ( contains( types, StoreType.PROPERTY ) )
        {
            config.put( GraphDatabaseSettings.array_block_size.name(),
                    String.valueOf( legacyStores.getPropertyStore().getArrayStore().getRecordDataSize() ) );
            config.put( GraphDatabaseSettings.string_block_size.name(),
                    String.valueOf( legacyStores.getPropertyStore().getStringStore().getRecordDataSize() ) );
        }
        if ( contains( types, StoreType.NODE_LABEL ) )
        {
            config.put( GraphDatabaseSettings.label_block_size.name(),
                    String.valueOf( legacyStores.getNodeStore().getDynamicLabelStore().getRecordDataSize() ) );
        }
        this.config.augment( config );
        return this.config;
    }
}
