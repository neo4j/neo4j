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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import java.io.File;
import java.io.IOException;
import javax.annotation.Resource;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Long.MAX_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor.NO_MONITOR;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.activeFilesBuilder;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.builder;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

@EnableRuleMigrationSupport
@ExtendWith( TestDirectoryExtension.class )
public class LogFilesBuilderTest
{
    @Resource
    public TestDirectory testDirectory;
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private File storeDirectory;
    private DefaultFileSystemAbstraction fileSystem;

    @BeforeEach
    public void setUp()
    {
        storeDirectory = testDirectory.directory();
        fileSystem = fileSystemRule.get();
    }

    @Test
    public void buildActiveFilesOnlyContext() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        TransactionLogFilesContext context = activeFilesBuilder( storeDirectory, fileSystem, pageCache ).buildContext();

        assertEquals( fileSystem, context.getFileSystem() );
        assertNotNull( context.getLogEntryReader() );
        assertSame( NO_MONITOR, context.getLogFileCreationMonitor() );
        assertEquals( MAX_VALUE, context.getRotationThreshold().get() );
        assertEquals( 0, context.getLastCommittedTransactionId() );
        assertEquals( 0, context.getLogVersionRepository().getCurrentLogVersion() );
    }

    @Test
    public void buildFilesBasedContext() throws IOException
    {
        TransactionLogFilesContext context = logFilesBasedOnlyBuilder( storeDirectory, fileSystem ).buildContext();
        assertEquals( fileSystem, context.getFileSystem() );
        assertSame( NO_MONITOR, context.getLogFileCreationMonitor() );
    }

    @Test
    public void buildDefaultContext() throws IOException
    {
        TransactionLogFilesContext context = builder( storeDirectory, fileSystem ).withLogVersionRepository( new SimpleLogVersionRepository( 2 ) )
                        .withTransactionIdStore( new SimpleTransactionIdStore() ).buildContext();
        assertEquals( fileSystem, context.getFileSystem() );
        assertNotNull( context.getLogEntryReader() );
        assertSame( NO_MONITOR, context.getLogFileCreationMonitor() );
        assertEquals( mebiBytes( 250 ), context.getRotationThreshold().get() );
        assertEquals( 1, context.getLastCommittedTransactionId() );
        assertEquals( 2, context.getLogVersionRepository().getCurrentLogVersion() );
    }

    @Test
    public void buildDefaultContextWithDependencies() throws IOException
    {
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository( 2 );
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( logVersionRepository );
        dependencies.satisfyDependency( transactionIdStore );

        TransactionLogFilesContext context =
                builder( storeDirectory, fileSystem ).withDependencies( dependencies ).buildContext();

        assertEquals( fileSystem, context.getFileSystem() );
        assertNotNull( context.getLogEntryReader() );
        assertSame( NO_MONITOR, context.getLogFileCreationMonitor() );
        assertEquals( mebiBytes( 250 ), context.getRotationThreshold().get() );
        assertEquals( 1, context.getLastCommittedTransactionId() );
        assertEquals( 2, context.getLogVersionRepository().getCurrentLogVersion() );
    }

    @Test
    public void buildContextWithCustomLogFilesLocations() throws Throwable
    {
        String customLogLocation = "customLogLocation";
        Config customLogLocationConfig = defaults( logical_logs_location, customLogLocation );
        LogFiles logFiles = builder( storeDirectory, fileSystem ).withConfig( customLogLocationConfig )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() ).build();
        logFiles.init();
        logFiles.start();

        assertEquals( new File( storeDirectory, customLogLocation ), logFiles.getHighestLogFile().getParentFile() );
    }

    @Test
    public void buildContextWithCustomAbsoluteLogFilesLocations() throws Throwable
    {
        File customLogDirectory = testDirectory.directory( "absoluteCustomLogDirectory" );
        Config customLogLocationConfig = defaults( logical_logs_location, customLogDirectory.getAbsolutePath() );
        LogFiles logFiles = builder( storeDirectory, fileSystem ).withConfig( customLogLocationConfig )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() ).build();
        logFiles.init();
        logFiles.start();

        assertEquals( customLogDirectory, logFiles.getHighestLogFile().getParentFile() );
    }

    @Test
    public void failToBuildFullContextWithoutLogVersionRepo()
    {
        assertThrows( NullPointerException.class, () -> {
            builder( storeDirectory, fileSystem ).withTransactionIdStore( new SimpleTransactionIdStore() ).buildContext();
        } );
    }

    @Test
    public void failToBuildFullContextWithoutTransactionIdStore()
    {
        assertThrows( NullPointerException.class, () -> {
            builder( storeDirectory, fileSystem ).withLogVersionRepository( new SimpleLogVersionRepository( 2 ) ).buildContext();
        } );
    }

    @Test
    public void fileBasedOperationsContextFailOnLastCommittedTransactionIdAccess()
    {
        assertThrows( UnsupportedOperationException.class, () -> {
            logFilesBasedOnlyBuilder( storeDirectory, fileSystem ).buildContext().getLastCommittedTransactionId();
        } );
    }

    @Test
    public void fileBasedOperationsContextFailOnLogVersionRepositoryAccess()
    {
        assertThrows( UnsupportedOperationException.class, () -> {
            logFilesBasedOnlyBuilder( storeDirectory, fileSystem ).buildContext().getLogVersionRepository();
        } );
    }
}
