/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.NeoStoreDataSourceRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class NeoStoreDataSourceTest
{
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTestWithEphemeralFS( fs.get(), getClass() );

    @Rule
    public NeoStoreDataSourceRule ds = new NeoStoreDataSourceRule();

    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void kernelHealthShouldBeHealedOnStart() throws Throwable
    {
        NeoStoreDataSource theDataSource = null;
        try
        {
            KernelHealth kernelHealth = new KernelHealth( mock( KernelPanicEventGenerator.class ),
                    NullLogProvider.getInstance().getLog( KernelHealth.class ) );

            theDataSource = ds.getDataSource( dir.graphDbDir(), fs.get(), pageCacheRule.getPageCache( fs.get() ),
                    stringMap(), kernelHealth );

            kernelHealth.panic( new Throwable() );

            theDataSource.start();

            kernelHealth.assertHealthy( Throwable.class );
        }
        finally
        {
            if ( theDataSource!= null )
            {
                theDataSource.stop();
                theDataSource.shutdown();
            }
        }
    }

    @Test
    public void shouldLogCorrectTransactionLogDiagnosticsForNoTransactionLogs() throws Exception
    {
        // GIVEN
        NeoStoreDataSource dataSource = neoStoreDataSourceWithLogFilesContainingLowestTxId( noLogs() );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Logger logger = logProvider.getLog( getClass() ).infoLogger();

        // WHEN
        NeoStoreDataSource.Diagnostics.TRANSACTION_RANGE.dump( dataSource, logger );

        // THEN
        logProvider.assertContainsMessageContaining( "No transactions" );
    }

    @Test
    public void shouldLogCorrectTransactionLogDiagnosticsForTransactionsInOldestLog() throws Exception
    {
        // GIVEN
        long logVersion = 2, prevLogLastTxId = 45;
        NeoStoreDataSource dataSource = neoStoreDataSourceWithLogFilesContainingLowestTxId(
                logWithTransactions( logVersion, prevLogLastTxId ) );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Logger logger = logProvider.getLog( getClass() ).infoLogger();

        // WHEN
        NeoStoreDataSource.Diagnostics.TRANSACTION_RANGE.dump( dataSource, logger );

        // THEN
        logProvider.assertContainsMessageContaining( "transaction " + (prevLogLastTxId + 1) );
        logProvider.assertContainsMessageContaining( "version " + logVersion );
    }

    @Test
    public void shouldLogCorrectTransactionLogDiagnosticsForTransactionsInSecondOldestLog() throws Exception
    {
        // GIVEN
        long logVersion = 2, prevLogLastTxId = 45;
        NeoStoreDataSource dataSource = neoStoreDataSourceWithLogFilesContainingLowestTxId(
                logWithTransactionsInNextToOldestLog( logVersion, prevLogLastTxId ) );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Logger logger = logProvider.getLog( getClass() ).infoLogger();

        // WHEN
        NeoStoreDataSource.Diagnostics.TRANSACTION_RANGE.dump( dataSource, logger );

        // THEN
        logProvider.assertContainsMessageContaining( "transaction " + (prevLogLastTxId + 1) );
        logProvider.assertContainsMessageContaining( "version " + (logVersion + 1) );
    }

    @Test
    public void logModuleSetUpError() throws Exception
    {
        Config config = new Config( stringMap(), GraphDatabaseSettings.class );
        StoreFactory storeFactory = mock( StoreFactory.class );
        Throwable openStoresError = new RuntimeException( "Can't set up modules" );
        when( storeFactory.openAllNeoStores( true ) ).thenThrow( openStoresError );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        CommunityIdTypeConfigurationProvider idTypeConfigurationProvider =
                new CommunityIdTypeConfigurationProvider();
        NeoStoreDataSource dataSource = ds.getDataSource( dir.graphDbDir(), fs.get(), config, storeFactory,
                new DefaultIdGeneratorFactory( fs.get(), idTypeConfigurationProvider ),
                idTypeConfigurationProvider, mock( KernelHealth.class ), logProvider );

        try
        {
            dataSource.start();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( openStoresError, e );
        }

        logProvider.assertAtLeastOnce( inLog( NeoStoreDataSource.class ).warn(
                equalTo( "Exception occurred while setting up store modules. Attempting to close things down." ),
                equalTo( openStoresError ) ) );
    }

    @Test
    public void logStartupError() throws Exception
    {
        Config config = new Config( stringMap(), GraphDatabaseSettings.class );
        StoreFactory storeFactory = mock( StoreFactory.class );
        NeoStores neoStores = mock( NeoStores.class, RETURNS_MOCKS );
        Throwable makeStoreOkError = new RuntimeException( "Can't make store ok" );
        doThrow( makeStoreOkError ).when( neoStores ).makeStoreOk();
        doReturn( neoStores ).when( storeFactory ).openAllNeoStores( true );

        AssertableLogProvider logProvider = new AssertableLogProvider();

        CommunityIdTypeConfigurationProvider idTypeConfigurationProvider =
                new CommunityIdTypeConfigurationProvider();
        NeoStoreDataSource dataSource = ds.getDataSource( dir.graphDbDir(), fs.get(), config, storeFactory,
                new DefaultIdGeneratorFactory( fs.get(), idTypeConfigurationProvider ), idTypeConfigurationProvider,
                mock( KernelHealth.class ), logProvider );

        Throwable dataSourceStartError = null;
        try
        {
            dataSource.start();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( makeStoreOkError, Exceptions.rootCause( e ) );
            dataSourceStartError = e;
        }

        logProvider.assertAtLeastOnce( inLog( NeoStoreDataSource.class ).warn(
                equalTo( "Exception occurred while starting the datasource. Attempting to close things down." ),
                equalTo( dataSourceStartError ) ) );
    }

    private NeoStoreDataSource neoStoreDataSourceWithLogFilesContainingLowestTxId( PhysicalLogFiles files )
    {
        DependencyResolver resolver = mock( DependencyResolver.class );
        when( resolver.resolveDependency( PhysicalLogFiles.class ) ).thenReturn( files );
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        when( dataSource.getDependencyResolver() ).thenReturn( resolver );
        return dataSource;
    }

    private PhysicalLogFiles noLogs()
    {
        PhysicalLogFiles files = mock( PhysicalLogFiles.class );
        when( files.getLowestLogVersion() ).thenReturn( -1L );
        return files;
    }

    private PhysicalLogFiles logWithTransactions( long logVersion, long headerTxId ) throws IOException
    {
        PhysicalLogFiles files = mock( PhysicalLogFiles.class );
        when( files.getLowestLogVersion() ).thenReturn( logVersion );
        when( files.hasAnyTransaction( logVersion ) ).thenReturn( true );
        when( files.versionExists( logVersion ) ).thenReturn( true );
        when( files.extractHeader( logVersion ) ).thenReturn( new LogHeader( LogEntryVersion.CURRENT.byteCode(),
                logVersion, headerTxId ) );
        return files;
    }

    private PhysicalLogFiles logWithTransactionsInNextToOldestLog( long logVersion, long prevLogLastTxId )
            throws IOException
    {
        PhysicalLogFiles files = logWithTransactions( logVersion + 1, prevLogLastTxId );
        when( files.getLowestLogVersion() ).thenReturn( logVersion );
        when( files.hasAnyTransaction( logVersion ) ).thenReturn( false );
        when( files.versionExists( logVersion ) ).thenReturn( true );
        return files;
    }
}
