/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.LongSupplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.NegativeIdException;
import org.neo4j.internal.id.ReservedIdException;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.Mask;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.IdValidator.INTEGER_MINUS_ONE;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

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
    private final Path storeFile = Path.of( "store" );
    private final Path idStoreFile = Path.of( "isStore" );
    private final RecordFormat<TheRecord> recordFormat = mock( RecordFormat.class );
    private final RecordIdType idType = RecordIdType.RELATIONSHIP; // whatever

    @Inject
    private PageCache pageCache;

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    @BeforeEach
    void setUpMocks() throws IOException
    {
        when( recordFormat.getPageSize( anyInt(), anyInt(), anyInt() ) ).thenReturn( Long.SIZE );
        when( idGeneratorFactory.open( any(), any( Path.class ), eq( idType ), any( LongSupplier.class ), anyLong(), any(), any(), any(), any(),
                any() ) ).thenReturn( idGenerator );
        when( pageFile.pageSize() ).thenReturn( PAGE_SIZE );
        when( pageFile.io( anyLong(), anyInt(), any() ) ).thenReturn( pageCursor );
        when( mockedPageCache.map( eq( storeFile ), anyInt(), any(), any() ) ).thenReturn( pageFile );
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
        Path storeFile = dir.file( "a" );
        Path idFile = dir.file( "idFile" );
        PageCache pageCache = mock( PageCache.class );
        PagedFile pagedFile = mock( PagedFile.class );
        PageCursor pageCursor = mock( PageCursor.class );

        when( pageCache.map( eq( storeFile ), anyInt(), any(), any() ) ).thenReturn( pagedFile );
        when( pagedFile.io( eq( 0L ), eq( PagedFile.PF_SHARED_READ_LOCK ), any() ) ).thenReturn( pageCursor );
        when( pageCursor.next() ).thenReturn( false );

        RecordFormats recordFormats = Standard.LATEST_RECORD_FORMATS;

        try ( DynamicArrayStore dynamicArrayStore = new DynamicArrayStore( storeFile, idFile, config, RecordIdType.NODE_LABELS, idGeneratorFactory, pageCache,
                NullLogProvider.getInstance(), GraphDatabaseInternalSettings.label_block_size.defaultValue(), recordFormats, writable(),
                databaseLayout.getDatabaseName(), immutable.empty() ) )
        {
            StoreNotFoundException storeNotFoundException = assertThrows( StoreNotFoundException.class, () -> dynamicArrayStore.initialise( false, NULL ) );
            assertEquals( "Fail to read header record of store file: " + storeFile.toAbsolutePath(), storeNotFoundException.getMessage() );
        }
    }

    @Test
    void throwsWhenRecordWithNegativeIdIsUpdated()
    {
        TheStore store = newStore();
        TheRecord record = newRecord( -1 );

        assertThrows( NegativeIdException.class, () ->
        {
            try ( var storeCursor = store.openPageCursorForWriting( 0, NULL ) )
            {
                store.updateRecord( record, storeCursor, NULL, StoreCursors.NULL );
            }
        } );
    }

    @Test
    void throwsWhenRecordWithTooHighIdIsUpdated()
    {
        long maxFormatId = 42;
        when( recordFormat.getMaxId() ).thenReturn( maxFormatId );

        TheStore store = newStore();
        TheRecord record = newRecord( maxFormatId + 1 );

        assertThrows( IdCapacityExceededException.class, () ->
        {
            try ( var storeCursor = store.openPageCursorForWriting( 0, NULL ) )
            {
                store.updateRecord( record, storeCursor, NULL, StoreCursors.NULL );
            }
        } );
    }

    @Test
    void throwsWhenRecordWithReservedIdIsUpdated()
    {
        TheStore store = newStore();
        TheRecord record = newRecord( INTEGER_MINUS_ONE );

        assertThrows( ReservedIdException.class, () ->
        {
            try ( var storeCursor = store.openPageCursorForWriting( 0, NULL ) )
            {
                store.updateRecord( record, storeCursor, NULL, StoreCursors.NULL );
            }
        } );
    }

    @Test
    void shouldDeleteOnCloseIfOpenOptionsSaysSo() throws IOException
    {
        // GIVEN
        Path nodeStore = databaseLayout.nodeStore();
        Path idFile =
                databaseLayout.idFile( RecordDatabaseFile.NODE_STORE ).orElseThrow( () -> new IllegalStateException( "Node store id file not found." ) );
        TheStore store = new TheStore( nodeStore, databaseLayout.idNodeStore(), config, idType,
                new DefaultIdGeneratorFactory( fs, immediate(), databaseLayout.getDatabaseName() ), pageCache, NullLogProvider.getInstance(), recordFormat,
                immutable.with( DELETE_ON_CLOSE ) );
        store.initialise( true, NULL );
        store.start( NULL );
        assertTrue( fs.fileExists( nodeStore ) );
        assertTrue( fs.fileExists( idFile ) );

        // WHEN
        store.close();

        // THEN
        assertFalse( fs.fileExists( nodeStore ) );
        assertFalse( fs.fileExists( idFile ) );
    }

    @Test
    public void shouldIncludeFileNameInIdUsagePrintout()
    {
        // given
        TheStore store = newStore();
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // when
        store.logIdUsage( logProvider.getLog( TheStore.class )::info, NULL );

        // then
        LogAssertions.assertThat( logProvider ).containsMessages( format( "%s[%s]: used=0 high=0", TheStore.TYPE_DESCRIPTOR, storeFile.getFileName() ) );
    }

    @Test
    public void shouldIncludeFileNameInStoreVersionPrintout()
    {
        // given
        TheStore store = newStore();
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // when
        store.logVersions( logProvider.getLog( TheStore.class )::info );

        // then
        LogAssertions.assertThat( logProvider ).containsMessages(
                format( "%s[%s] %s", TheStore.TYPE_DESCRIPTOR, storeFile.getFileName(), TheStore.STORE_VERSION ) );
    }

    private TheStore newStore()
    {
        LogProvider log = NullLogProvider.getInstance();
        TheStore store = new TheStore( storeFile, idStoreFile, config, idType, idGeneratorFactory, mockedPageCache, log, recordFormat, immutable.empty() );
        store.initialise( false, NULL );
        return store;
    }

    private static TheRecord newRecord( long id )
    {
        return new TheRecord( id );
    }

    private static class TheStore extends CommonAbstractStore<TheRecord,NoStoreHeader>
    {
        static final String TYPE_DESCRIPTOR = "TheType";
        static final String STORE_VERSION = "v1";

        TheStore( Path file, Path idFile, Config configuration, RecordIdType idType, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
                LogProvider logProvider, RecordFormat<TheRecord> recordFormat, ImmutableSet<OpenOption> openOptions )
        {
            super( file, idFile, configuration, idType, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR, recordFormat,
                    NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT, STORE_VERSION, writable(), DEFAULT_DATABASE_NAME, openOptions );
        }

        @Override
        protected void initialiseNewStoreFile( CursorContext cursorContext )
        {
        }

        @Override
        protected int determineRecordSize()
        {
            return RECORD_SIZE;
        }

        @Override
        public long scanForHighId( CursorContext cursorContext )
        {
            return HIGH_ID;
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

        @Override
        public String toString( Mask mask )
        {
            return String.format( "TheRecord[%d]", getId() );
        }
    }
}
