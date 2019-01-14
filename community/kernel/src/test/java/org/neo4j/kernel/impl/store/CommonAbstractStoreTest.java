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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.function.LongSupplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.validation.IdCapacityExceededException;
import org.neo4j.kernel.impl.store.id.validation.NegativeIdException;
import org.neo4j.kernel.impl.store.id.validation.ReservedIdException;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.ConfigurablePageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.neo4j.test.rule.TestDirectory.testDirectory;

public class CommonAbstractStoreTest
{
    private static final int PAGE_SIZE = 32;
    private static final int RECORD_SIZE = 10;
    private static final int HIGH_ID = 42;

    private final IdGenerator idGenerator = mock( IdGenerator.class );
    private final IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
    private final PageCursor pageCursor = mock( PageCursor.class );
    private final PagedFile pageFile = mock( PagedFile.class );
    private final PageCache pageCache = mock( PageCache.class );
    private final Config config = Config.defaults();
    private final File storeFile = new File( "store" );
    private final File idStoreFile = new File( "isStore" );
    private final RecordFormat<TheRecord> recordFormat = mock( RecordFormat.class );
    private final IdType idType = IdType.RELATIONSHIP; // whatever

    private static final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private static final TestDirectory dir = testDirectory( fileSystemRule.get() );
    private static final ConfigurablePageCacheRule pageCacheRule = new ConfigurablePageCacheRule();

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( dir ).around( pageCacheRule );

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUpMocks() throws IOException
    {
        when( idGeneratorFactory.open( any( File.class ), eq( idType ), any( LongSupplier.class ), anyLong() ) )
                .thenReturn( idGenerator );

        when( pageFile.pageSize() ).thenReturn( PAGE_SIZE );
        when( pageFile.io( anyLong(), anyInt() ) ).thenReturn( pageCursor );
        when( pageCache.map( eq( storeFile ), anyInt() ) ).thenReturn( pageFile );
    }

    @Test
    public void shouldCloseStoreFileFirstAndIdGeneratorAfter() throws Throwable
    {
        // given
        TheStore store = newStore();
        InOrder inOrder = inOrder( pageFile, idGenerator );

        // when
        store.close();

        // then
        inOrder.verify( pageFile, times( 1 ) ).close();
        inOrder.verify( idGenerator, times( 1 ) ).close();
    }

    @Test
    public void failStoreInitializationWhenHeaderRecordCantBeRead() throws IOException
    {
        File storeFile = dir.file( "a" );
        File idFile = dir.file( "idFile" );
        PageCache pageCache = mock( PageCache.class );
        PagedFile pagedFile = mock( PagedFile.class );
        PageCursor pageCursor = mock( PageCursor.class );

        when( pageCache.map( eq( storeFile ), anyInt(), any( OpenOption.class ) ) ).thenReturn( pagedFile );
        when( pagedFile.io( 0L, PagedFile.PF_SHARED_READ_LOCK ) ).thenReturn( pageCursor );
        when( pageCursor.next() ).thenReturn( false );

        RecordFormats recordFormats = Standard.LATEST_RECORD_FORMATS;

        expectedException.expect( StoreNotFoundException.class );
        expectedException.expectMessage( "Fail to read header record of store file: " + storeFile.getAbsolutePath() );

        try ( DynamicArrayStore dynamicArrayStore = new DynamicArrayStore( storeFile, idFile, config, IdType.NODE_LABELS,
                idGeneratorFactory, pageCache, NullLogProvider.getInstance(),
                Settings.INTEGER.apply( GraphDatabaseSettings.label_block_size.getDefaultValue() ), recordFormats ) )
        {
            dynamicArrayStore.initialise( false );
        }
    }

    @Test
    public void throwsWhenRecordWithNegativeIdIsUpdated()
    {
        TheStore store = newStore();
        TheRecord record = newRecord( -1 );

        try
        {
            store.updateRecord( record );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NegativeIdException.class ) );
        }
    }

    @Test
    public void throwsWhenRecordWithTooHighIdIsUpdated()
    {
        long maxFormatId = 42;
        when( recordFormat.getMaxId() ).thenReturn( maxFormatId );

        TheStore store = newStore();
        TheRecord record = newRecord( maxFormatId + 1 );

        try
        {
            store.updateRecord( record );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IdCapacityExceededException.class ) );
        }
    }

    @Test
    public void throwsWhenRecordWithReservedIdIsUpdated()
    {
        TheStore store = newStore();
        TheRecord record = newRecord( IdGeneratorImpl.INTEGER_MINUS_ONE );

        try
        {
            store.updateRecord( record );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ReservedIdException.class ) );
        }
    }

    @Test
    public void shouldDeleteOnCloseIfOpenOptionsSaysSo()
    {
        // GIVEN
        DatabaseLayout databaseLayout = dir.databaseLayout();
        File nodeStore = databaseLayout.nodeStore();
        File idFile = databaseLayout.idFile( DatabaseFile.NODE_STORE ).orElseThrow( () -> new IllegalStateException( "Node store id file not found." ) );
        FileSystemAbstraction fs = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs, Config.defaults() );
        TheStore store = new TheStore( nodeStore, databaseLayout.idNodeStore(), config, idType, new DefaultIdGeneratorFactory( fs ), pageCache,
                NullLogProvider.getInstance(), recordFormat, DELETE_ON_CLOSE );
        store.initialise( true );
        store.makeStoreOk();
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
        TheStore store = new TheStore( storeFile, idStoreFile, config, idType, idGeneratorFactory, pageCache, log, recordFormat );
        store.initialise( false );
        return store;
    }

    private TheRecord newRecord( long id )
    {
        return new TheRecord( id );
    }

    private static class TheStore extends CommonAbstractStore<TheRecord,NoStoreHeader>
    {
        TheStore( File file, File idFile, Config configuration, IdType idType, IdGeneratorFactory idGeneratorFactory,
                PageCache pageCache, LogProvider logProvider, RecordFormat<TheRecord> recordFormat,
                OpenOption... openOptions )
        {
            super( file, idFile, configuration, idType, idGeneratorFactory, pageCache, logProvider, "TheType",
                    recordFormat, NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT, "v1", openOptions );
        }

        @Override
        protected void initialiseNewStoreFile( PagedFile file )
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
        public TheRecord clone()
        {
            return new TheRecord( getId() );
        }
    }
}
