/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.jmx.impl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.ByteBuffer;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.jmx.StoreFile;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.DefaultKernelData;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.spi.legacyindex.IndexImplementation;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.iterable;

public class StoreFileBeanTest
{
    private FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private File storeDir = new File( "" );
    private PhysicalLogFiles physicalLogFiles = new PhysicalLogFiles( storeDir, fs );
    private LegacyIndexProviderLookup legacyIndexProviderLookup = mock( LegacyIndexProviderLookup.class );
    private SchemaIndexProvider schemaIndexProvider = mock( SchemaIndexProvider.class );
    private LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private StoreFile storeFileBean;

    @Before
    public void setUp() throws Throwable
    {
        LogFile logFile = mock( LogFile.class );
        DataSourceManager dataSourceManager = new DataSourceManager();
        GraphDatabaseAPI db = mock( GraphDatabaseAPI.class );
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );

        // Setup all dependencies
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( fs );
        dependencies.satisfyDependencies( dataSourceManager );
        dependencies.satisfyDependency( logFile );
        dependencies.satisfyDependency( physicalLogFiles );
        dependencies.satisfyDependency( legacyIndexProviderLookup );
        dependencies.satisfyDependency( schemaIndexProvider );
        dependencies.satisfyDependency( labelScanStore );
        when( db.getDependencyResolver() ).thenReturn( dependencies );
        when( dataSource.getDependencyResolver() ).thenReturn( dependencies );

        // Start DataSourceManager
        when( dataSource.getStoreDir() ).thenReturn( storeDir );
        dataSourceManager.register( dataSource );
        dataSourceManager.start();

        // Create bean
        KernelData kernelData = new DefaultKernelData( fs, mock( PageCache.class ), storeDir, Config.defaults(), db );
        ManagementData data = new ManagementData( new StoreFileBean(), kernelData, ManagementSupport.load() );
        storeFileBean = (StoreFile) new StoreFileBean().createMBean( data );
    }

    @Test
    public void shouldCountAllLogFiles() throws Throwable
    {
        try ( StoreChannel storeChannel = fs.create( physicalLogFiles.getLogFileForVersion( 0 ) ) )
        {
            byte[] bytes = new byte[10];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }

        try ( StoreChannel storeChannel = fs.create( physicalLogFiles.getLogFileForVersion( 1 ) ) )
        {
            byte[] bytes = new byte[20];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }

        assertEquals( 30, storeFileBean.getAllLogicalLogsSize() );
    }

    @Test
    public void shouldCountAllIndexFiles() throws Exception
    {
        // Legacy index file
        File legacyIndex = new File( storeDir, "legacyIndex" );
        try ( StoreChannel storeChannel = fs.create( legacyIndex ) )
        {
            byte[] bytes = new byte[10];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }

        IndexImplementation indexImplementation = mock( IndexImplementation.class );
        when( indexImplementation.getIndexImplementationDirectory( Mockito.any() ) ).thenReturn( legacyIndex );
        when( legacyIndexProviderLookup.all() ).thenReturn( iterable( indexImplementation ) );

        // Legacy index file
        File schemaIndex = new File( storeDir, "schemaIndex" );
        try ( StoreChannel storeChannel = fs.create( schemaIndex ) )
        {
            byte[] bytes = new byte[5];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }
        when( schemaIndexProvider.getSchemaIndexStoreDirectory( Mockito.any() ) ).thenReturn( schemaIndex );

        // Label scan store
        File labelScan = new File( storeDir, "labelScanStore" );
        try ( StoreChannel storeChannel = fs.create( labelScan ) )
        {
            byte[] bytes = new byte[20];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }
        when( labelScanStore.getLabelScanStoreFile() ).thenReturn( labelScan );

        // Count all files
        assertEquals( 35, storeFileBean.getIndexStoreSize() );
    }
}
