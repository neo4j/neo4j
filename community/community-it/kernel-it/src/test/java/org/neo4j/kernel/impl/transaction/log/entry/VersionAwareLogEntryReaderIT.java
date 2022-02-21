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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.file.Files;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

@DbmsExtension
class VersionAwareLogEntryReaderIT
{
    // this offset includes log header and transaction that create node on test setup
    // Magic number represents number of bytes that log file is actually using (in form of header size + payload)
    // to be able to check that its like that or to update manually you can disable pre-allocation + some manual checks.
    private static final long END_OF_DATA_OFFSET = CURRENT_FORMAT_LOG_HEADER_SIZE + 3443L;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseManagementService managementService;
    private DatabaseLayout databaseLayout;
    private VersionAwareLogEntryReader entryReader;
    private StorageEngineFactory storageEngineFactory;

    @BeforeEach
    void setUp()
    {
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        createNode( database );
        GraphDatabaseAPI dbApi = (GraphDatabaseAPI) database;
        databaseLayout = dbApi.databaseLayout();
        storageEngineFactory = dbApi.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
        managementService.shutdown();
    }

    @Test
    @EnabledOnOs( OS.LINUX )
    void readOnlyLogFilesWhileCommandsAreAvailable() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withStorageEngineFactory( storageEngineFactory )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            getLastReadablePosition( logFiles );
            assertEquals( kibiBytes( 128 ), Files.size( logFiles.getLogFile().getHighestLogFile() ) );
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals( 0L, logPosition.getLogVersion() );
            // this position in a log file before 0's are actually starting
            assertEquals( END_OF_DATA_OFFSET, logPosition.getByteOffset() );
        }
    }

    @Test
    void correctlyResetPositionWhenEndOfCommandsReached() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withStorageEngineFactory( storageEngineFactory )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {

            for ( int i = 0; i < 10; i++ )
            {
                assertEquals( END_OF_DATA_OFFSET, getLastReadablePosition( logFiles ) );
            }
        }
    }

    @Test
    @DisabledOnOs( OS.LINUX )
    void readTillTheEndOfNotPreallocatedFile() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withStorageEngineFactory( storageEngineFactory )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            getLastReadablePosition( logFiles );
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals( 0L, logPosition.getLogVersion() );
            assertEquals( Files.size( logFiles.getLogFile().getHighestLogFile() ), logPosition.getByteOffset() );
        }
    }

    private long getLastReadablePosition( LogFiles logFiles ) throws IOException
    {
        var logFile = logFiles.getLogFile();
        try ( ReadableLogChannel logChannel = logFile.getReader( logFile.extractHeader( 0 ).getStartPosition() ) )
        {
            while ( entryReader.readLogEntry( logChannel ) != null )
            {
                // read to the end
            }
            return entryReader.lastPosition().getByteOffset();
        }
    }

    private static void createNode( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }
}
