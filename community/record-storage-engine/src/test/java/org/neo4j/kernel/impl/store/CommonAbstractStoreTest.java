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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.function.LongSupplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.NegativeIdException;
import org.neo4j.internal.id.ReservedIdException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.IdValidator.INTEGER_MINUS_ONE;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

@PageCacheExtension
@Neo4jLayoutExtension
class CommonAbstractStoreTest
{
    private static final int PAGE_SIZE = 32;
    private static final int RECORD_SIZE = 10;
    private static final int HIGH_ID = 42;

    private final IdGenerator idGenerator = mock( IdGenerator.class );
    private final IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
    private final PageCursor pageCursor = mock( PageCursor.class );
    private final PagedFile pageFile = mock( PagedFile.class );
    private final PageCache mockedPageCache = mock( PageCache.class );
    private final Config config = Config.defaults();
    private final File storeFile = new File( "store" );
    private final File idStoreFile = new File( "isStore" );
    private final RecordFormat<TheRecord> recordFormat = mock( RecordFormat.class );
    private final IdType idType = IdType.RELATIONSHIP; // whatever

    @Inject
    private PageCache pageCache;

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    @Inject
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUpMocks() throws IOException
    {
        when( idGeneratorFactory.open( any(), any( File.class ), eq( idType ), any( LongSupplier.class ), anyLong(), anyBoolean() ) ).thenReturn( idGenerator );
        when( pageFile.pageSize() ).thenReturn( PAGE_SIZE );
        when( pageFile.io( anyLong(), anyInt(), any() ) ).thenReturn( pageCursor );
        when( mockedPageCache.map( eq( storeFile ), anyInt() ) ).thenReturn( pageFile );
    }

    @Test
    void shouldCloseStoreFileFirstAndIdGeneratorAfter()
    {
        // given
        TheStore store = newStore();
        InOrder inOrder = inOrder( pageFile, idGenerator );

        // when
        store.close();

        // then
        inOrder.verify( pageFile ).close();
        inOrder.verify( idGenerator ).close();
    }

    @Test
    void failStoreInitializationWhenHeaderRecordCantBeRead() throws IOException
    {
        File storeFile = dir.file( "a" );
        File idFile = dir.file( "idFile" );
        PageCache pageCache = mock( PageCache.class );
        PagedFile pagedFile = mock( PagedFile.class );
        PageCursor pageCursor = mock( PageCursor.class );

        when( pageCache.map( eq( storeFile ), anyInt(), any( OpenOption.class ) ) ).thenReturn( pagedFile );
        when( pagedFile.io( 0L, PagedFile.PF_SHARED_READ_LOCK, TRACER_SUPPLIER.get() ) ).thenReturn( pageCursor );
        when( pageCursor.next() ).thenReturn( false );

        RecordFormats recordFormats = Standard.LATEST_RECORD_FORMATS;

        try ( DynamicArrayStore dynamicArrayStore = new DynamicArrayStore( storeFile, idFile, config, IdType.NODE_LABELS, idGeneratorFactory, pageCache,
                NullLogProvider.getInstance(), GraphDatabaseSettings.label_block_size.defaultValue(), recordFormats ) )
        {
            StoreNotFoundException storeNotFoundException = assertThrows( StoreNotFoundException.class, () -> dynamicArrayStore.initialise( false ) );
            assertEquals( "Fail to read header record of store file: " + storeFile.getAbsolutePath(), storeNotFoundException.getMessage() );
        }
    }

    @Test
    void throwsWhenRecordWithNegativeIdIsUpdated()
    {
        TheStore store = newStore();
        TheRecord record = newRecord( -1 );

        assertThrows( NegativeIdException.class, () -> store.updateRecord( record ) );
    }

    @Test
    void throwsWhenRecordWithTooHighIdIsUpdated()
    {
        long maxFormatId = 42;
        when( recordFormat.getMaxId() ).thenReturn( maxFormatId );

        TheStore store = newStore();
        TheRecord record = newRecord( maxFormatId + 1 );

        assertThrows( IdCapacityExceededException.class, () -> store.updateRecord( record ) );
    }

    @Test
    void throwsWhenRecordWithReservedIdIsUpdated()
    {
        TheStore store = newStore();
        TheRecord record = newRecord( INTEGER_MINUS_ONE );

        assertThrows( ReservedIdException.class, () -> store.updateRecord( record ) );
    }

    @Test
    void shouldDeleteOnCloseIfOpenOptionsSaysSo() throws IOException
    {
        // GIVEN
        File nodeStore = databaseLayout.nodeStore();
        File idFile = databaseLayout.idFile( DatabaseFile.NODE_STORE ).orElseThrow( () -> new IllegalStateException( "Node store id file not found." ) );
        TheStore store = new TheStore( nodeStore, databaseLayout.idNodeStore(), config, idType, new DefaultIdGeneratorFactory( fs, immediate() ),
                pageCache, NullLogProvider.getInstance(), recordFormat, DELETE_ON_CLOSE );
        store.initialise( true );
        store.start();
        assertTrue( fs.fileExists( nodeStore ) );
        assertTrue( fs.fileExists( idFile ) );

        // WHEN
        store.close();

        // THEN
        assertFalse( fs.fileExists( nodeStore ) );
        assertFalse( fs.fileExists( idFile ) );
    }

    private TheStore newStore()
    {
        LogProvider log = NullLogProvider.getInstance();
        TheStore store = new TheStore( storeFile, idStoreFile, config, idType, idGeneratorFactory, mockedPageCache, log, recordFormat );
        store.initialise( false );
        return store;
    }

    private TheRecord newRecord( long id )
    {
        return new TheRecord( id );
    }

    private static class TheStore extends CommonAbstractStore<TheRecord,NoStoreHeader>
    {
        TheStore( File file, File idFile, Config configuration, IdType idType, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
                LogProvider logProvider, RecordFormat<TheRecord> recordFormat, OpenOption... openOptions )
        {
            super( file, idFile, configuration, idType, idGeneratorFactory, pageCache, logProvider, "TheType", recordFormat,
                    NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT, "v1", openOptions );
        }

        @Override
        protected void initialiseNewStoreFile()
        {
        }

        @Override
        protected int determineRecordSize()
        {
            return RECORD_SIZE;
        }

        @Override
        public long scanForHighId()
        {
            return HIGH_ID;
        }

        @Override
        public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, TheRecord record )
        {
        }
    }

    private static class TheRecord extends AbstractBaseRecord
    {
        TheRecord( long id )
        {
            super( id );
        }

        @Override
        public AbstractBaseRecord copy()
        {
            return super.copy();
        }
    }
}
