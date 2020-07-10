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
package org.neo4j.kernel.database;

import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.neo4j.collection.Dependencies;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.DatabaseFileHelper.filesToKeepOnTruncation;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;

@TestDirectoryExtension
@DbmsExtension( configurationCallback = "configure" )
class DatabaseIT
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private Database database;

    private PageCacheWrapper pageCacheWrapper;
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        Dependencies dependencies = new Dependencies();
        pageCacheWrapper = new PageCacheWrapper( pageCacheExtension.getPageCache( fs ) );
        dependencies.satisfyDependency( pageCacheWrapper );
        builder.setInternalLogProvider( logProvider ).setExternalDependencies( dependencies );
    }

    @Test
    void databaseHealthShouldBeHealedOnStart() throws Throwable
    {
        database.stop();

        database.init();
        var databaseHealth = database.getDatabaseHealth();
        databaseHealth.panic( new Throwable() );

        database.start();
        databaseHealth.assertHealthy( Throwable.class );
    }

    @Test
    void dropDataOfNotStartedDatabase()
    {
        database.stop();

        assertNotEquals( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory() );
        assertTrue( fs.fileExists( databaseLayout.databaseDirectory().toFile() ) );
        assertTrue( fs.fileExists( databaseLayout.getTransactionLogsDirectory().toFile() ) );

        database.drop();

        assertFalse( fs.fileExists( databaseLayout.databaseDirectory().toFile() ) );
        assertFalse( fs.fileExists( databaseLayout.getTransactionLogsDirectory().toFile() ) );
    }

    @Test
    void truncateNotStartedDatabase()
    {
        database.stop();

        File databaseDirectory = databaseLayout.databaseDirectory().toFile();
        File transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory().toFile();
        assertTrue( fs.fileExists( databaseDirectory ) );
        assertTrue( fs.fileExists( transactionLogsDirectory ) );

        File[] databaseFilesShouldExist = filesToKeepOnTruncation( databaseLayout ).stream().map( Path::toFile ).filter( File::exists ).toArray( File[]::new );

        database.truncate();

        assertTrue( fs.fileExists( databaseDirectory ) );
        assertTrue( fs.fileExists( transactionLogsDirectory ) );
        File[] currentDatabaseFiles = databaseDirectory.listFiles();
        assertThat( currentDatabaseFiles ).contains( databaseFilesShouldExist );
    }

    @Test
    void doNotFlushDataFilesOnDatabaseTruncate() throws IOException
    {
        database.start();
        var mappingsBefore = new ArrayList<>( pageCacheWrapper.listExistingMappings() );

        database.truncate();

        var removedFiles = new ArrayList<>( pageCacheWrapper.listExistingMappings() );
        removedFiles.removeAll( mappingsBefore );
        var removedPaths = removedFiles.stream().map( PagedFile::path ).collect( Collectors.toList() );

        DatabaseLayout databaseLayout = database.getDatabaseLayout();
        Set<Path> files = databaseLayout.storeFiles();
        files.removeAll( filesToKeepOnTruncation( databaseLayout ) );
        Path[] filesShouldBeDeleted = files.stream().filter( Files::exists ).toArray( Path[]::new );
        assertThat( removedPaths ).contains( filesShouldBeDeleted );
    }

    @Test
    void filesRecreatedAfterTruncate()
    {
        File databaseDirectory = databaseLayout.databaseDirectory().toFile();
        File transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory().toFile();
        assertTrue( fs.fileExists( databaseDirectory ) );
        assertTrue( fs.fileExists( transactionLogsDirectory ) );

        File[] databaseFilesBeforeTruncate = fs.listFiles( databaseDirectory );
        File[] logFilesBeforeTruncate = fs.listFiles( transactionLogsDirectory );

        database.truncate();

        assertTrue( fs.fileExists( databaseDirectory ) );
        assertTrue( fs.fileExists( transactionLogsDirectory ) );

        File[] databaseFiles = fs.listFiles( databaseDirectory );
        File[] logFiles = fs.listFiles( transactionLogsDirectory );

        // files are equal by name - every store file is recreated as result
        assertThat( databaseFilesBeforeTruncate ).contains( databaseFiles );
        assertThat( logFilesBeforeTruncate ).contains( logFiles );
    }

    @Test
    void noPageCacheFlushOnDatabaseDrop()
    {
        database.start();
        int flushesBefore = pageCacheWrapper.getFlushes();

        database.drop();

        assertEquals( flushesBefore, pageCacheWrapper.getFlushes() );
    }

    @Test
    void removeDatabaseDataAndLogsOnDrop()
    {
        assertNotEquals( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory() );
        assertTrue( fs.fileExists( databaseLayout.databaseDirectory().toFile() ) );
        assertTrue( fs.fileExists( databaseLayout.getTransactionLogsDirectory().toFile() ) );

        database.drop();

        assertFalse( fs.fileExists( databaseLayout.databaseDirectory().toFile() ) );
        assertFalse( fs.fileExists( databaseLayout.getTransactionLogsDirectory().toFile() ) );
    }

    @Test
    void flushDatabaseDataOnStop()
    {
        int flushesBeforeClose = pageCacheWrapper.getFileFlushes();

        database.stop();

        assertNotEquals( flushesBeforeClose, pageCacheWrapper.getFileFlushes() );
    }

    @Test
    void flushOfThePageCacheHappensOnlyOnceDuringShutdown() throws Throwable
    {
        int databaseFiles = (int) database.getDatabaseFileListing().builder().build().stream().count();
        int flushesBefore = pageCacheWrapper.getFlushes();
        int filesFlushesBefore = pageCacheWrapper.getFileFlushes();
        database.stop();

        assertEquals( flushesBefore, pageCacheWrapper.getFlushes() );
        assertThat( pageCacheWrapper.getFileFlushes() ).isGreaterThanOrEqualTo( filesFlushesBefore + databaseFiles );
    }

    @Test
    void flushOfThePageCacheOnShutdownDoesNotHappenIfTheDbIsUnhealthy() throws Throwable
    {
        var databaseHealth = database.getDatabaseHealth();
        databaseHealth.panic( new Throwable( "Critical failure" ) );
        int fileFlushesBefore = pageCacheWrapper.getFileFlushes();
        int databaseFiles = (int) database.getDatabaseFileListing().builder().excludeLogFiles().build().stream().count();

        database.stop();

        assertThat( pageCacheWrapper.getFileFlushes() ).isLessThan( fileFlushesBefore + databaseFiles );
    }

    @Test
    void logModuleSetUpError()
    {
        var exception = new RuntimeException( "StartupError" );

        database.stop();
        database.init();

        database.getLife().add( LifecycleAdapter.onStart( () ->
        {
            throw exception;
        } ) );

        var e = assertThrows( Exception.class, () -> database.start() );
        assertThat( e ).hasRootCause( exception );

        LogAssertions.assertThat( logProvider ).forClass( Database.class ).forLevel( WARN )
              .containsMessageWithException( "Exception occurred while starting the database. Trying to stop already started components.", e.getCause() );
    }

    @Test
    void shouldAlwaysShutdownLifeEvenWhenSomeComponentFailing()
    {
        var expectedException = new RuntimeException( "Failure" );
        var life = database.getLife();
        var availabilityGuard = database.getDatabaseAvailabilityGuard();
        life.add( LifecycleAdapter.onShutdown( () ->
        {
            throw expectedException;
        } ) );
        var e = assertThrows( Throwable.class, () -> database.stop() );
        assertThat( e ).hasCause( expectedException );
        assertFalse( availabilityGuard.isAvailable() );
    }

    @Test
    void shouldHaveDatabaseLogServiceInDependencyResolver()
    {
        var logService = database.getDependencyResolver().resolveDependency( LogService.class );
        assertEquals( database.getLogService(), logService );
        assertThat( logService ).isInstanceOf( DatabaseLogService.class );
    }

    private static class PageCacheWrapper extends DelegatingPageCache
    {
        private final AtomicInteger flushes = new AtomicInteger();
        private final AtomicInteger fileFlushes = new AtomicInteger();

        PageCacheWrapper( PageCache delegate )
        {
            super( delegate );
        }

        @Override
        public PagedFile map( Path path, int pageSize ) throws IOException
        {
            return new PageFileWrapper( super.map( path, pageSize ), fileFlushes );
        }

        @Override
        public PagedFile map( Path path, VersionContextSupplier versionContextSupplier, int pageSize, ImmutableSet<OpenOption> openOptions ) throws IOException
        {
            return new PageFileWrapper( super.map( path, versionContextSupplier, pageSize, openOptions ), fileFlushes );
        }

        @Override
        public PagedFile map( Path path, int pageSize, ImmutableSet<OpenOption> openOptions ) throws IOException
        {
            return new PageFileWrapper( super.map( path, pageSize, openOptions ), fileFlushes );
        }

        @Override
        public PagedFile map( Path path, VersionContextSupplier versionContextSupplier, int pageSize, ImmutableSet<OpenOption> openOptions,
                String databaseName ) throws IOException
        {
            return new PageFileWrapper( super.map( path, versionContextSupplier, pageSize, openOptions, databaseName ), fileFlushes );
        }

        @Override
        public void flushAndForce( IOLimiter limiter ) throws IOException
        {
            flushes.incrementAndGet();
            super.flushAndForce( limiter );
        }

        @Override
        public void flushAndForce() throws IOException
        {
            flushes.incrementAndGet();
            super.flushAndForce();
        }

        public int getFlushes()
        {
            return flushes.get();
        }

        public int getFileFlushes()
        {
            return fileFlushes.get();
        }
    }

    private static class PageFileWrapper extends DelegatingPagedFile
    {

        private final AtomicInteger flushCounter;

        PageFileWrapper( PagedFile delegate, AtomicInteger flushCounter )
        {
            super( delegate );
            this.flushCounter = flushCounter;
        }

        @Override
        public void flushAndForce() throws IOException
        {
            flushCounter.incrementAndGet();
            super.flushAndForce();
        }

        @Override
        public void flushAndForce( IOLimiter limiter ) throws IOException
        {
            flushCounter.incrementAndGet();
            super.flushAndForce( limiter );
        }
    }
}
