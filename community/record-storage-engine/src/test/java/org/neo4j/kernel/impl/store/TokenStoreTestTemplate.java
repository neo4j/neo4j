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
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
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

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
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
    private void setUp() throws IOException
    {
        File file = dir.file( "label-tokens.db" );
        File idFile = dir.file( "label-tokens.db.id" );
        File namesFile = dir.file( "label-tokens.db.names" );
        File namesIdFile = dir.file( "label-tokens.db.names.id" );

        IdGeneratorFactory generatorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        LogProvider logProvider = NullLogProvider.getInstance();

        RecordFormats formats = RecordFormatSelector.defaultFormat();
        Config config = Config.defaults();
        nameStore = new DynamicStringStore( namesFile, namesIdFile, config, IdType.LABEL_TOKEN_NAME, generatorFactory, pageCache, logProvider,
                TokenStore.NAME_STORE_BLOCK_SIZE, formats.dynamic(), formats.storeVersion() );
        store = instantiateStore( file, idFile, generatorFactory, pageCache, logProvider, nameStore, formats, config );
        nameStore.initialise( true );
        store.initialise( true );
        nameStore.start();
        store.start();
    }

    protected abstract TokenStore<R> instantiateStore( File file, File idFile, IdGeneratorFactory generatorFactory, PageCache pageCache,
            LogProvider logProvider, DynamicStringStore nameStore, RecordFormats formats, Config config );

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
        assertThat( readBack ).isEqualTo( tokenRecord );
        assertThat( tokenRecord.isInternal() ).isEqualTo( false );
        assertThat( readBack.isInternal() ).isEqualTo( false );
    }

    @Test
    void tokensMustPreserveTheirInternalFlag()
    {
        R tokenRecord = createInUseRecord( allocateNameRecords( "MyInternalToken" ) );
        tokenRecord.setInternal( true );
        storeToken( tokenRecord );

        R readBack = store.getRecord( tokenRecord.getId(), store.newRecord(), NORMAL );
        store.ensureHeavy( readBack );
        assertThat( readBack ).isEqualTo( tokenRecord );
        assertThat( tokenRecord.isInternal() ).isEqualTo( true );
        assertThat( readBack.isInternal() ).isEqualTo( true );

        NamedToken token = store.getToken( toIntExact( tokenRecord.getId() ) );
        assertThat( token.name() ).isEqualTo( "MyInternalToken" );
        assertThat( token.id() ).isIn( toIntExact( tokenRecord.getId() ) );
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

        assertThat( readA ).isEqualTo( tokenA );
        assertThat( readA.isInternal() ).isEqualTo( tokenA.isInternal() );
        assertThat( readB ).isEqualTo( tokenB );
        assertThat( readB.isInternal() ).isEqualTo( tokenB.isInternal() );
        assertThat( readC ).isEqualTo( tokenC );
        assertThat( readC.isInternal() ).isEqualTo( tokenC.isInternal() );
        assertThat( readD ).isEqualTo( tokenD );
        assertThat( readD.isInternal() ).isEqualTo( tokenD.isInternal() );

        Iterator<NamedToken> itr = store.getAllReadableTokens().iterator();
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenA", 0 ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenB", 1 ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenC", 2, true ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenD", 3 ) );

        itr = store.getTokens().iterator();
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenA", 0 ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenB", 1 ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenC", 2, true ) );
        assertTrue( itr.hasNext() );
        assertThat( itr.next() ).isEqualTo( new NamedToken( "TokenD", 3 ) );
    }

    private R createInUseRecord( List<DynamicRecord> nameRecords )
    {
        R tokenRecord = store.newRecord();
        tokenRecord.setId( store.nextId() );
        tokenRecord.initialize( true, nameRecords.get( 0 ).getIntId() );
        tokenRecord.addNameRecords( nameRecords );
        tokenRecord.setCreated();
        return tokenRecord;
    }

    private void createEmptyPageZero() throws IOException
    {
        try ( PageCursor cursor = store.pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, PageCursorTracer.NULL ) )
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
