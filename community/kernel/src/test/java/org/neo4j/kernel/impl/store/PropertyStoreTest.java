/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.JumpingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.test.rule.PageCacheConfig.config;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class PropertyStoreTest
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( config().withInconsistentReads( false ) );
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private File storeFile;
    private File idFile;

    @BeforeEach
    void setup()
    {
        storeFile = testDirectory.databaseLayout().propertyStore();
        idFile = testDirectory.databaseLayout().idPropertyStore();
    }

    @Test
    void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord()
    {
        // given
        PageCache pageCache = pageCacheExtension.getPageCache( fs );
        Config config = Config.defaults( GraphDatabaseSettings.rebuild_idgenerators_fast, "true" );

        DynamicStringStore stringPropertyStore = mock( DynamicStringStore.class );

        final PropertyStore store =
                new PropertyStore( storeFile, idFile, config, new JumpingIdGeneratorFactory( 1 ), pageCache,
                        NullLogProvider.getInstance(), stringPropertyStore, mock( PropertyKeyTokenStore.class ), mock( DynamicArrayStore.class ),
                        RecordFormatSelector.defaultFormat() );
        store.initialise( true );

        try
        {
            store.makeStoreOk();
            final long propertyRecordId = store.nextId();

            PropertyRecord record = new PropertyRecord( propertyRecordId );
            record.setInUse( true );

            DynamicRecord dynamicRecord = dynamicRecord();
            PropertyBlock propertyBlock = propertyBlockWith( dynamicRecord );
            record.setPropertyBlock( propertyBlock );

            doAnswer( invocation ->
            {
                PropertyRecord recordBeforeWrite = store.getRecord( propertyRecordId, store.newRecord(), FORCE );
                assertFalse( recordBeforeWrite.inUse() );
                return null;
            } ).when( stringPropertyStore ).updateRecord( dynamicRecord );

            // when
            store.updateRecord( record );

            // then verify that our mocked method above, with the assert, was actually called
            verify( stringPropertyStore ).updateRecord( dynamicRecord );
        }
        finally
        {
            store.close();
        }
    }

    private static DynamicRecord dynamicRecord()
    {
        DynamicRecord dynamicRecord = new DynamicRecord( 42 );
        dynamicRecord.setType( PropertyType.STRING.intValue() );
        dynamicRecord.setCreated();
        return dynamicRecord;
    }

    private static PropertyBlock propertyBlockWith( DynamicRecord dynamicRecord )
    {
        PropertyBlock propertyBlock = new PropertyBlock();

        PropertyKeyTokenRecord key = new PropertyKeyTokenRecord( 10 );
        propertyBlock.setSingleBlock( key.getId() | (((long) PropertyType.STRING.intValue()) << 24) | (dynamicRecord
                .getId() << 28) );
        propertyBlock.addValueRecord( dynamicRecord );

        return propertyBlock;
    }
}
