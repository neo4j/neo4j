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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.activeFilesBuilder;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.builder;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

public class LogFilesBuilderTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private File directory;
    private EphemeralFileSystemAbstraction fileSystem;

    @Before
    public void setUp() throws Exception
    {
        directory = testDirectory.directory();
        fileSystem = fileSystemRule.get();
    }

    @Test
    public void buildActiveFilesOnlyContext() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        TransactionLogFilesContext context = activeFilesBuilder( directory, fileSystem, pageCache ).buildContext();

        assertEquals( fileSystem, context.getFileSystem() );
        assertNotNull( context.getLogEntryReader() );
        assertSame( LogFileCreationMonitor.NO_MONITOR, context.getLogFileCreationMonitor() );
        assertEquals( Long.MAX_VALUE, context.getRotationThreshold() );
        assertEquals( 0, context.getLastCommittedTransactionId() );
        assertEquals( 0, context.getLogVersionRepository().getCurrentLogVersion() );
    }

    @Test
    public void buildFilesBasedContext() throws IOException
    {
        TransactionLogFilesContext context = logFilesBasedOnlyBuilder( directory, fileSystem ).buildContext();
        assertEquals( fileSystem, context.getFileSystem() );
        assertSame( LogFileCreationMonitor.NO_MONITOR, context.getLogFileCreationMonitor() );
    }

    @Test
    public void buildDefaultContext() throws IOException
    {
        TransactionLogFilesContext context =
                builder( directory, fileSystem ).withLogVersionRepository( new SimpleLogVersionRepository( 2 ) )
                        .withTransactionIdStore( new SimpleTransactionIdStore() ).buildContext();
        assertEquals( fileSystem, context.getFileSystem() );
        assertNotNull( context.getLogEntryReader() );
        assertSame( LogFileCreationMonitor.NO_MONITOR, context.getLogFileCreationMonitor() );
        assertEquals( ByteUnit.mebiBytes( 250 ), context.getRotationThreshold() );
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
                builder( directory, fileSystem ).withDependencies( dependencies ).buildContext();

        assertEquals( fileSystem, context.getFileSystem() );
        assertNotNull( context.getLogEntryReader() );
        assertSame( LogFileCreationMonitor.NO_MONITOR, context.getLogFileCreationMonitor() );
        assertEquals( ByteUnit.mebiBytes( 250 ), context.getRotationThreshold() );
        assertEquals( 1, context.getLastCommittedTransactionId() );
        assertEquals( 2, context.getLogVersionRepository().getCurrentLogVersion() );
    }

    @Test( expected = NullPointerException.class )
    public void failToBuildFullContextWithoutLogVersionRepo() throws IOException
    {
        builder( directory, fileSystem ).withTransactionIdStore( new SimpleTransactionIdStore() ).buildContext();
    }

    @Test( expected = NullPointerException.class )
    public void failToBuildFullContextWithoutTransactionIdStore() throws IOException
    {
        builder( directory, fileSystem ).withLogVersionRepository( new SimpleLogVersionRepository( 2 ) ).buildContext();
    }

    @Test( expected = UnsupportedOperationException.class )
    public void fileBasedOperationsContextFailOnLastCommittedTransactionIdAccess() throws IOException
    {
        logFilesBasedOnlyBuilder( directory, fileSystem ).buildContext().getLastCommittedTransactionId();
    }

    @Test( expected = UnsupportedOperationException.class )
    public void fileBasedOperationsContextFailOnLogVersionRepositoryAccess() throws IOException
    {
        logFilesBasedOnlyBuilder( directory, fileSystem ).buildContext().getLogVersionRepository();
    }
}
