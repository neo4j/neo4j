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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@EphemeralPageCacheExtension
abstract class TokenStoreTestTemplate<R extends TokenRecord>
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory dir;
    @Inject
    private PageCache pageCache;

    private TokenStore<R> store;
    private DynamicStringStore nameStore;

    @BeforeEach
    private void setUp()
    {
        File file = dir.createFile( "label-tokens.db" );
        File idFile = dir.createFile( "label-tokens.db.id" );
        File namesFile = dir.createFile( "label-tokens.db.names" );
        File namesIdFile = dir.createFile( "label-tokens.db.names.id" );

        IdGeneratorFactory generatorFactory = new DefaultIdGeneratorFactory( fs );
        LogProvider logProvider = NullLogProvider.getInstance();

        RecordFormats formats = RecordFormatSelector.defaultFormat();
        Config config = Config.defaults();
        nameStore = new DynamicStringStore( namesFile, namesIdFile, config, IdType.LABEL_TOKEN_NAME, generatorFactory, pageCache, logProvider,
                TokenStore.NAME_STORE_BLOCK_SIZE, formats.dynamic(), formats.storeVersion() );
        store = createStore( file, idFile, generatorFactory, pageCache, logProvider, nameStore, formats, config );
        nameStore.initialise( true );
        store.initialise( true );
        nameStore.makeStoreOk();
        store.makeStoreOk();
    }

    protected abstract TokenStore<R> createStore( File file, File idFile, IdGeneratorFactory generatorFactory, PageCache pageCache, LogProvider logProvider,
            DynamicStringStore nameStore, RecordFormats formats, Config config );

    @AfterEach
    private void tearDown() throws IOException
    {
        IOUtils.closeAll( store, nameStore );
    }

    @Test
    void forceGetRecordSkipInUseCheck() throws IOException
    {
        createEmptyPageZero();
        R record = store.getRecord( 7, store.newRecord(), FORCE );
        assertFalse( "Record should not be in use", record.inUse() );
    }

    @Test
    void getRecordWithNormalModeMustThrowIfTheRecordIsNotInUse() throws IOException
    {
        createEmptyPageZero();

        Assertions.assertThrows( InvalidRecordException.class, () ->
        {
            store.getRecord( 7, store.newRecord(), NORMAL );
        } );
    }

    @Test
    void tokensMustNotBeInternalByDefault()
    {
        List<DynamicRecord> nameRecords = allocateNameRecords( "MyToken" );
        R tokenRecord = createInUseRecord( nameRecords );
        for ( DynamicRecord nameRecord : nameRecords )
        {
            nameStore.updateRecord( nameRecord );
        }
        store.updateRecord( tokenRecord );

        R readBack = store.getRecord( tokenRecord.getId(), store.newRecord(), NORMAL );
        assertThat( readBack, is( equalTo( tokenRecord ) ) );
        assertThat( tokenRecord.isInternal(), is( false ) );
        assertThat( readBack.isInternal(), is( false ) );
    }

    @Test
    void tokensMustPreserveTheirInternalFlag()
    {
        List<DynamicRecord> nameRecords = allocateNameRecords( "MyInternalToken" );
        R tokenRecord = createInUseRecord( nameRecords );
        tokenRecord.setInternal( true );
        for ( DynamicRecord nameRecord : nameRecords )
        {
            nameStore.updateRecord( nameRecord );
        }
        store.updateRecord( tokenRecord );

        R readBack = store.getRecord( tokenRecord.getId(), store.newRecord(), NORMAL );
        assertThat( readBack, is( equalTo( tokenRecord ) ) );
        assertThat( tokenRecord.isInternal(), is( true ) );
        assertThat( readBack.isInternal(), is( true ) );
    }

    private R createInUseRecord( List<DynamicRecord> nameRecords )
    {
        R tokenRecord = store.newRecord();
        tokenRecord.setId( store.nextId() );
        tokenRecord.initialize( true, nameRecords.get( 0 ).getIntId() );
        tokenRecord.addNameRecords( nameRecords );
        return tokenRecord;
    }

    private void createEmptyPageZero() throws IOException
    {
        try ( PageCursor cursor = store.pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            // Create an empty page in the file. All records in here will look like they are unused.
            assertTrue( cursor.next() );
        }
    }

    private List<DynamicRecord> allocateNameRecords( String tokenName )
    {
        List<DynamicRecord> nameRecords = new ArrayList<>();
        nameStore.allocateRecordsFromBytes( nameRecords, tokenName.getBytes( StandardCharsets.UTF_8 ) );
        return nameRecords;
    }
}
