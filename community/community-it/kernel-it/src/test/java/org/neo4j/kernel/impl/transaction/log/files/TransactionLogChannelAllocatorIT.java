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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.internal.nativeimpl.LinuxNativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.dynamic_read_only_failover;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@Isolated
class TransactionLogChannelAllocatorIT
{
    private static final long ROTATION_THRESHOLD = ByteUnit.mebiBytes( 25 );
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    private TransactionLogFilesHelper fileHelper;
    private TransactionLogChannelAllocator fileAllocator;
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final Config config = Config.defaults();

    @BeforeEach
    void setUp()
    {
        fileHelper = new TransactionLogFilesHelper( fileSystem, testDirectory.homePath() );
        fileAllocator = createLogFileAllocator();
    }

    @Test
    void rawChannelDoesNotTryToAdviseOnFileContent() throws IOException
    {
        Path path = fileHelper.getLogFileForVersion( 1 );
        try ( var storeChannel = fileSystem.write( path ) )
        {
            writeLogHeader( storeChannel, new LogHeader( CURRENT_LOG_FORMAT_VERSION, 1, 1, 1 ), INSTANCE );
        }

        var logHeaderCache = new LogHeaderCache( 10 );
        var logFileContext = createLogFileContext();
        var nativeChannelAccessor = new AdviseCountingChannelNativeAccessor();
        var channelAllocator = new TransactionLogChannelAllocator( logFileContext, fileHelper, logHeaderCache, nativeChannelAccessor );
        try ( var channel = channelAllocator.openLogChannel( 1, true ) )
        {
            assertEquals( 0, nativeChannelAccessor.getCallCounter() );
        }
    }

    @Test
    void defaultChannelTryToAdviseOnFileContent() throws IOException
    {
        Path path = fileHelper.getLogFileForVersion( 1 );
        try ( StoreChannel storeChannel = fileSystem.write( path ) )
        {
            writeLogHeader( storeChannel, new LogHeader( CURRENT_LOG_FORMAT_VERSION, 1, 1, 1 ), INSTANCE );
        }

        var logHeaderCache = new LogHeaderCache( 10 );
        var logFileContext = createLogFileContext();
        var nativeChannelAccessor = new AdviseCountingChannelNativeAccessor();
        var channelAllocator = new TransactionLogChannelAllocator( logFileContext, fileHelper, logHeaderCache, nativeChannelAccessor );
        try ( var channel = channelAllocator.openLogChannel( 1 ) )
        {
            assertEquals( 1, nativeChannelAccessor.getCallCounter() );
        }
    }

    @Test
    @EnabledOnOs( OS.LINUX )
    void allocateNewTransactionLogFile() throws IOException
    {
        PhysicalLogVersionedStoreChannel logChannel = fileAllocator.createLogChannel( 10, () -> 1L );
        assertEquals( ROTATION_THRESHOLD, logChannel.size() );
    }

    @Test
    void failToPreallocateFileWithOutOfDiskSpaceError() throws IOException
    {
        var logFileContext = createLogFileContext( getUnavailableBytes(), new PreallocationFailingChannelNativeAccess() );
        var nativeChannelAccessor = new LogFileChannelNativeAccessor( fileSystem, logFileContext );
        var unreasonableAllocator = new TransactionLogChannelAllocator( logFileContext, fileHelper, new LogHeaderCache( 10 ), nativeChannelAccessor );
        var exception = assertThrows( RuntimeException.class, () -> unreasonableAllocator.createLogChannel( 10, () -> 1L ) );
        assertThat( exception ).hasRootCauseInstanceOf( ReadOnlyDbException.class )
                .hasRootCauseMessage( "This Neo4j instance is read only for the database neo4j" );

        assertThat( logProvider.serialize() ).containsSequence( "Warning! System is running out of disk space. " +
                "Failed to preallocate log file since disk does not have enough space left. Please provision more space to avoid that." );
    }

    @Test
    void failToPreallocateFileWithOutOfDiskSpaceErrorAndDisabledFailover() throws IOException
    {
        config.setDynamic( dynamic_read_only_failover, false, "test" );
        var logFileContext = createLogFileContext( getUnavailableBytes(), new PreallocationFailingChannelNativeAccess() );
        var nativeChannelAccessor = new LogFileChannelNativeAccessor( fileSystem, logFileContext );
        var unreasonableAllocator = new TransactionLogChannelAllocator( logFileContext, fileHelper, new LogHeaderCache( 10 ), nativeChannelAccessor );
        try ( PhysicalLogVersionedStoreChannel channel = unreasonableAllocator.createLogChannel( 10, () -> 1L ) )
        {
            assertEquals( CURRENT_FORMAT_LOG_HEADER_SIZE, channel.size() );
            assertThat( logProvider.serialize() ).containsSequence(
                    "Warning! System is running out of disk space. Failed to preallocate log file since disk does " +
                            "not have enough space left. Please provision more space to avoid that." ).containsSequence(
                    "Dynamic switchover to read-only mode is disabled. The database will continue execution in the current mode." );
        }
    }

    @Test
    @DisabledOnOs( OS.LINUX )
    void allocateNewTransactionLogFileOnSystemThatDoesNotSupportPreallocations() throws IOException
    {
        try ( PhysicalLogVersionedStoreChannel logChannel = fileAllocator.createLogChannel( 10, () -> 1L ) )
        {
            assertEquals( CURRENT_FORMAT_LOG_HEADER_SIZE, logChannel.size() );
        }
    }

    @Test
    void openExistingFileDoesNotPerformAnyAllocations() throws IOException
    {
        Path file = fileHelper.getLogFileForVersion( 11 );
        fileSystem.write( file ).close();

        TransactionLogChannelAllocator fileAllocator = createLogFileAllocator();
        try ( PhysicalLogVersionedStoreChannel channel = fileAllocator.createLogChannel( 11, () -> 1L ) )
        {
            assertEquals( CURRENT_FORMAT_LOG_HEADER_SIZE, channel.size() );
        }
    }

    private long getUnavailableBytes() throws IOException
    {
        return Files.getFileStore( testDirectory.homePath() ).getUsableSpace() + ByteUnit.gibiBytes( 10 );
    }

    private TransactionLogChannelAllocator createLogFileAllocator()
    {
        return createLogFileAllocator( ROTATION_THRESHOLD );
    }

    private TransactionLogFilesContext createLogFileContext()
    {
        return createLogFileContext( ROTATION_THRESHOLD );
    }

    private TransactionLogChannelAllocator createLogFileAllocator( long rotationsThreshold )
    {
        LogHeaderCache logHeaderCache = new LogHeaderCache( 10 );
        var logFileContext = createLogFileContext( rotationsThreshold );
        var nativeChannelAccessor = new LogFileChannelNativeAccessor( fileSystem, logFileContext );
        return new TransactionLogChannelAllocator( logFileContext, fileHelper, logHeaderCache, nativeChannelAccessor );
    }

    private TransactionLogFilesContext createLogFileContext( long rotationThreshold )
    {
        return createLogFileContext( rotationThreshold, NativeAccessProvider.getNativeAccess() );
    }

    private TransactionLogFilesContext createLogFileContext( long rotationThreshold, NativeAccess nativeAccess )
    {
        return new TransactionLogFilesContext( new AtomicLong( rotationThreshold ), new AtomicBoolean( true ),
                new VersionAwareLogEntryReader( new TestCommandReaderFactory() ), () -> 1L,
                () -> 1L, () -> new LogPosition( 0, 1 ),
                SimpleLogVersionRepository::new, fileSystem, logProvider, DatabaseTracers.EMPTY, () -> StoreId.UNKNOWN,
                nativeAccess, INSTANCE, new Monitors(), true,
                new DatabaseHealth( PanicEventGenerator.NO_OP, NullLog.getInstance() ), () -> KernelVersion.LATEST,
                Clock.systemUTC(), DEFAULT_DATABASE_NAME, config );
    }

    private static class AdviseCountingChannelNativeAccessor extends ChannelNativeAccessor.EmptyChannelNativeAccessor
    {
        private long callCounter;

        @Override
        public void adviseSequentialAccessAndKeepInCache( StoreChannel storeChannel, long version )
        {
            callCounter++;
        }

        public long getCallCounter()
        {
            return callCounter;
        }
    }

    private static class PreallocationFailingChannelNativeAccess extends LinuxNativeAccess
    {
        @Override
        public NativeCallResult tryPreallocateSpace( int fd, long bytes )
        {
            return new NativeCallResult( 28, "Sometimes science is more art than science" );
        }
    }
}
