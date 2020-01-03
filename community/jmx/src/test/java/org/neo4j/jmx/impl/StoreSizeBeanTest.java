/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.jmx.impl;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.jmx.StoreSize;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.ExplicitIndexProvider;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.collection.Iterables.iterable;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class StoreSizeBeanTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    private final ExplicitIndexProvider explicitIndexProviderLookup = mock( ExplicitIndexProvider.class );
    private final IndexProvider indexProvider = mockedIndexProvider( "provider1" );
    private final IndexProvider indexProvider2 = mockedIndexProvider( "provider2" );
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private StoreSize storeSizeBean;
    private LogFiles logFiles;
    private ManagementData managementData;

    @BeforeEach
    void setUp() throws IOException
    {
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( testDirectory.databaseDir(), fs ).build();

        Dependencies dependencies = new Dependencies();
        Config config = Config.defaults( default_schema_provider, indexProvider.getProviderDescriptor().name() );
        DataSourceManager dataSourceManager = new DataSourceManager( config );
        GraphDatabaseAPI db = mock( GraphDatabaseAPI.class );
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );

        dependencies.satisfyDependency( indexProvider );
        dependencies.satisfyDependency( indexProvider2 );

        DefaultIndexProviderMap indexProviderMap = new DefaultIndexProviderMap( dependencies, config );
        indexProviderMap.init();

        // Setup all dependencies
        dependencies.satisfyDependency( fs );
        dependencies.satisfyDependencies( dataSourceManager );
        dependencies.satisfyDependency( logFiles );
        dependencies.satisfyDependency( explicitIndexProviderLookup );
        dependencies.satisfyDependency( indexProviderMap );
        dependencies.satisfyDependency( labelScanStore );
        when( db.getDependencyResolver() ).thenReturn( dependencies );
        when( dataSource.getDependencyResolver() ).thenReturn( dependencies );
        when( dataSource.getDatabaseLayout() ).thenReturn( testDirectory.databaseLayout() );

        // Start DataSourceManager
        dataSourceManager.register( dataSource );
        dataSourceManager.start();

        // Create bean
        KernelData kernelData = new KernelData( fs, mock( PageCache.class ), testDirectory.databaseDir(), config, dataSourceManager );
        managementData = new ManagementData( new StoreSizeBean(), kernelData, ManagementSupport.load() );
        storeSizeBean = StoreSizeBean.createBean( managementData, false, 0, mock( Clock.class ) );

        when( indexProvider.directoryStructure() ).thenReturn( mock( IndexDirectoryStructure.class ) );
        when( indexProvider2.directoryStructure() ).thenReturn( mock( IndexDirectoryStructure.class ) );
        when( labelScanStore.getLabelScanStoreFile() ).thenReturn( testDirectory.databaseLayout().labelScanStore() );
    }

    private static IndexProvider mockedIndexProvider( String name )
    {
        IndexProvider provider = mock( IndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( new IndexProviderDescriptor( name, "1" ) );
        return provider;
    }

    private void createFakeStoreDirectory() throws IOException
    {
        Map<File, Integer> dummyStore = new HashMap<>();
        DatabaseLayout layout = testDirectory.databaseLayout();
        dummyStore.put( layout.nodeStore(), 1 );
        dummyStore.put( layout.idNodeStore(), 2 );
        dummyStore.put( layout.nodeLabelStore(), 3 );
        dummyStore.put( layout.idNodeLabelStore(), 4 );
        dummyStore.put( layout.propertyStore(), 5 );
        dummyStore.put( layout.idPropertyStore(), 6 );
        dummyStore.put( layout.propertyKeyTokenStore(), 7 );
        dummyStore.put( layout.idPropertyKeyTokenStore(), 8 );
        dummyStore.put( layout.propertyKeyTokenNamesStore(), 9 );
        dummyStore.put( layout.idPropertyKeyTokenNamesStore(), 10 );
        dummyStore.put( layout.propertyStringStore(), 11 );
        dummyStore.put( layout.idPropertyStringStore(), 12 );
        dummyStore.put( layout.propertyArrayStore(), 13 );
        dummyStore.put( layout.idPropertyArrayStore(), 14 );
        dummyStore.put( layout.relationshipStore(), 15 );
        dummyStore.put( layout.idRelationshipStore(), 16 );
        dummyStore.put( layout.relationshipGroupStore(), 17 );
        dummyStore.put( layout.idRelationshipGroupStore(), 18 );
        dummyStore.put( layout.relationshipTypeTokenStore(), 19 );
        dummyStore.put( layout.idRelationshipTypeTokenStore(), 20 );
        dummyStore.put( layout.relationshipTypeTokenNamesStore(), 21 );
        dummyStore.put( layout.idRelationshipTypeTokenNamesStore(), 22 );
        dummyStore.put( layout.labelTokenStore(), 23 );
        dummyStore.put( layout.idLabelTokenStore(), 24 );
        dummyStore.put( layout.labelTokenNamesStore(), 25 );
        dummyStore.put( layout.idLabelTokenNamesStore(), 26 );
        dummyStore.put( layout.schemaStore(), 27 );
        dummyStore.put( layout.idSchemaStore(), 28 );
        dummyStore.put( layout.countStoreB(), 29 );
        // COUNTS_STORE_B is created in the test

        for ( Map.Entry<File, Integer> fileEntry : dummyStore.entrySet() )
        {
            createFileOfSize( fileEntry.getKey(), fileEntry.getValue() );
        }
    }

    @Test
    void verifyGroupingOfNodeRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 1, 4 ), storeSizeBean.getNodeStoreSize() );
    }

    @Test
    void verifyGroupingOfPropertyRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 5, 10 ), storeSizeBean.getPropertyStoreSize() );
    }

    @Test
    void verifyGroupingOfStringRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 11, 12 ), storeSizeBean.getStringStoreSize() );
    }

    @Test
    void verifyGroupingOfArrayRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 13, 14 ), storeSizeBean.getArrayStoreSize() );
    }

    @Test
    void verifyGroupingOfRelationshipRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 15, 22 ), storeSizeBean.getRelationshipStoreSize() );
    }

    @Test
    void verifyGroupingOfLabelRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 23, 26 ), storeSizeBean.getLabelStoreSize() );
    }

    @Test
    void verifyGroupingOfCountStoreRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 29, 29 ), storeSizeBean.getCountStoreSize() );
        createFileOfSize( testDirectory.databaseLayout().countStoreA(), 30 );
        assertEquals( getExpected( 29, 30 ), storeSizeBean.getCountStoreSize() );
    }

    @Test
    void verifyGroupingOfSchemaRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 27, 28 ), storeSizeBean.getSchemaStoreSize() );
    }

    @Test
    void sumAllFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 0, 29 ), storeSizeBean.getTotalStoreSize() );
    }

    @Test
    void shouldCountAllLogFiles() throws Throwable
    {
        createFileOfSize( logFiles.getLogFileForVersion( 0 ), 1 );
        createFileOfSize( logFiles.getLogFileForVersion( 1 ), 2 );

        assertEquals( 3L, storeSizeBean.getTransactionLogsSize() );
    }

    @Test
    void shouldCountAllIndexFiles() throws Exception
    {
        // Explicit index file
        File explicitIndex = testDirectory.databaseLayout().file( "explicitIndex" );
        createFileOfSize( explicitIndex, 1 );

        IndexImplementation indexImplementation = mock( IndexImplementation.class );
        when( indexImplementation.getIndexImplementationDirectory( any() ) ).thenReturn( explicitIndex );
        when( explicitIndexProviderLookup.allIndexProviders() ).thenReturn( iterable( indexImplementation ) );

        // Schema index files
        File schemaIndex = testDirectory.databaseLayout().file( "schemaIndex" );
        createFileOfSize( schemaIndex, 2 );
        IndexDirectoryStructure directoryStructure = mock( IndexDirectoryStructure.class );
        when( directoryStructure.rootDirectory() ).thenReturn( schemaIndex );
        when( indexProvider.directoryStructure() ).thenReturn( directoryStructure );

        File schemaIndex2 = testDirectory.databaseLayout().file( "schemaIndex2" );
        createFileOfSize( schemaIndex2, 3 );
        IndexDirectoryStructure directoryStructure2 = mock( IndexDirectoryStructure.class );
        when( directoryStructure2.rootDirectory() ).thenReturn( schemaIndex2 );
        when( indexProvider2.directoryStructure() ).thenReturn( directoryStructure2 );

        // Label scan store
        File labelScan = testDirectory.databaseLayout().labelScanStore();
        createFileOfSize( labelScan, 4 );
        when( labelScanStore.getLabelScanStoreFile() ).thenReturn( labelScan );

        // Count all files
        assertEquals( 10, storeSizeBean.getIndexStoreSize() );
    }

    @Test
    void shouldCacheValues() throws IOException
    {
        final Clock clock = mock( Clock.class );
        storeSizeBean = StoreSizeBean.createBean( managementData, false, 100, clock );
        when( clock.millis() ).thenReturn( 100L );

        createFileOfSize( logFiles.getLogFileForVersion( 0 ), 1 );
        createFileOfSize( logFiles.getLogFileForVersion( 1 ), 2 );

        Assert.assertEquals( 3L, storeSizeBean.getTransactionLogsSize() );

        createFileOfSize( logFiles.getLogFileForVersion( 2 ), 3 );
        createFileOfSize( logFiles.getLogFileForVersion( 3 ), 4 );

        Assert.assertEquals( 3L, storeSizeBean.getTransactionLogsSize() );

        when( clock.millis() ).thenReturn( 200L );

        Assert.assertEquals( 10L, storeSizeBean.getTransactionLogsSize() );
    }

    private void createFileOfSize( File file, int size ) throws IOException
    {
        try ( StoreChannel storeChannel = fs.create( file ) )
        {
            byte[] bytes = new byte[size];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }
    }

    private static long getExpected( int lower, int upper )
    {
        long expected = 0;
        for ( int i = lower; i <= upper; i++ )
        {
            expected += i;
        }
        return expected;
    }
}
