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
import java.util.Iterator;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.api.NamedToken;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        assertFalse( record.inUse(), "Record should not be in use" );
    }

    @Test
    void getRecordWithNormalModeMustThrowIfTheRecordIsNotInUse() throws IOException
    {
        createEmptyPageZero();

        Assertions.assertThrows( InvalidRecordException.class, () -> store.getRecord( 7, store.newRecord(), NORMAL ) );
    }

    @Test
    void tokensMustNotBeInternalByDefault()
    {
        R tokenRecord = createInUseRecord( allocateNameRecords( "MyToken" ) );
        storeToken( tokenRecord );

        R readBack = store.getRecord( tokenRecord.getId(), store.newRecord(), NORMAL );
        store.ensureHeavy( readBack );
        assertThat( readBack, is( equalTo( tokenRecord ) ) );
        assertThat( tokenRecord.isInternal(), is( false ) );
        assertThat( readBack.isInternal(), is( false ) );
    }

    @Test
    void tokensMustPreserveTheirInternalFlag()
    {
        R tokenRecord = createInUseRecord( allocateNameRecords( "MyInternalToken" ) );
        tokenRecord.setInternal( true );
        storeToken( tokenRecord );

        R readBack = store.getRecord( tokenRecord.getId(), store.newRecord(), NORMAL );
        store.ensureHeavy( readBack );
        assertThat( readBack, is( equalTo( tokenRecord ) ) );
        assertThat( tokenRecord.isInternal(), is( true ) );
        assertThat( readBack.isInternal(), is( true ) );

        NamedToken token = store.getToken( Math.toIntExact( tokenRecord.getId() ) );
        assertThat( token.name(), is( "MyInternalToken" ) );
        assertThat( token.id(), is( Math.toIntExact( tokenRecord.getId() ) ) );
        assertTrue( token.isInternal() );
    }

    @Test
    void gettingAllReadableTokensAndAllTokensMustAlsoReturnTokensThatAreInternal()
    {
        R tokenA = createInUseRecord( allocateNameRecords( "TokenA" ) );
        R tokenB = createInUseRecord( allocateNameRecords( "TokenB" ) );
        R tokenC = createInUseRecord( allocateNameRecords( "TokenC" ) );
        tokenC.setInternal( true );
        R tokenD = createInUseRecord( allocateNameRecords( "TokenD" ) );

        storeToken( tokenA );
        storeToken( tokenB );
        storeToken( tokenC );
        storeToken( tokenD );

        R readA = store.getRecord( tokenA.getId(), store.newRecord(), NORMAL );
        R readB = store.getRecord( tokenB.getId(), store.newRecord(), NORMAL );
        R readC = store.getRecord( tokenC.getId(), store.newRecord(), NORMAL );
        R readD = store.getRecord( tokenD.getId(), store.newRecord(), NORMAL );
        store.ensureHeavy( readA );
        store.ensureHeavy( readB );
        store.ensureHeavy( readC );
        store.ensureHeavy( readD );

        assertThat( readA, is( equalTo( tokenA ) ) );
        assertThat( readA.isInternal(), is( tokenA.isInternal() ) );
        assertThat( readB, is( equalTo( tokenB ) ) );
        assertThat( readB.isInternal(), is( tokenB.isInternal() ) );
        assertThat( readC, is( equalTo( tokenC ) ) );
        assertThat( readC.isInternal(), is( tokenC.isInternal() ) );
        assertThat( readD, is( equalTo( tokenD ) ) );
        assertThat( readD.isInternal(), is( tokenD.isInternal() ) );

        Iterator<NamedToken> itr = store.getAllReadableTokens().iterator();
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenA", 0 ) ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenB", 1 ) ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenC", 2, true ) ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenD", 3 ) ) );

        itr = store.getTokens().iterator();
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenA", 0 ) ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenB", 1 ) ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenC", 2, true ) ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next(), is( new NamedToken( "TokenD", 3 ) ) );
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

    private void storeToken( R tokenRecord )
    {
        for ( DynamicRecord nameRecord : tokenRecord.getNameRecords() )
        {
            nameStore.updateRecord( nameRecord );
        }
        store.updateRecord( tokenRecord );
    }
}
