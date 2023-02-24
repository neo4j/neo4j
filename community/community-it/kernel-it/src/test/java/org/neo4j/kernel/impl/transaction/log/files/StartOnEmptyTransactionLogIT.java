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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.function.Consumers;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@Neo4jLayoutExtension
public class StartOnEmptyTransactionLogIT
{
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabaseManagementServiceBuilder builder;
    private LogFiles logFiles;
    private DatabaseManagementService dbms;

    @BeforeEach
    void setup() throws IOException
    {
        builder = new TestDatabaseManagementServiceBuilder( neo4jLayout );
        var dbms = builder.build();
        dbms.shutdown();

        var system = neo4jLayout.databaseLayout( SYSTEM_DATABASE_NAME );
        logFiles = buildLogFiles( system );
        // No checkpoint entries
        truncate( logFiles.getCheckpointFile().getCurrentFile(), LogVersions.LOG_HEADER_SIZE_4_0 );
        // Empty tx log
        truncate( logFiles.getLogFile().getHighestLogFile(), 0 );
        assertThat( getTxLogFileSize() ).isEqualTo( 0 );
    }

    @AfterEach
    void tearDown()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
        }
    }

    /**
     * This test verifies that we cannot start on transaction logs with size 0
     */
    @Test
    void shouldNotStartOnEmptyTransactionLogs()
    {
        var exception = Assertions.assertThrows( RuntimeException.class, () -> dbms( Consumers.ignoreValue() ) );
        assertThat( exception ).getRootCause().isInstanceOf( IncompleteLogHeaderException.class );
    }

    /**
     * This test verifies that we can work around transaction logs with size 0
     * by using unsupported.dbms.tx_log.fail_on_corrupted_log_files=false.
     * From https://github.com/neo4j/neo4j/issues/13048
     */
    @Test
    void shouldStartOnEmptyTransactionWhenConfiguredToNotFailOnCorruptedLogFiles() throws IOException
    {
        dbms = dbms( builder -> builder.setConfig( GraphDatabaseInternalSettings.fail_on_corrupted_log_files, false ) );

        assertThat( getTxLogFileSize() ).isGreaterThan( 0 );
        var systemDb = dbms.database( SYSTEM_DATABASE_NAME );
        assertThatNoException().isThrownBy( () ->
                                            {
                                                try ( Transaction tx = systemDb.beginTx() )
                                                {
                                                    tx.createNode();
                                                    tx.commit();
                                                }
                                            } );
    }

    @EnabledOnOs( OS.LINUX )
    @Test
    void shouldPreAllocateOnExistingButEmptyTxLogFile() throws IOException
    {
        var preAllocationSize = ByteUnit.kibiBytes( 128 );
        dbms = dbms( builder -> builder
                .setConfig( GraphDatabaseInternalSettings.fail_on_corrupted_log_files, false )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, preAllocationSize )
        );

        assertThat( getTxLogFileSize() ).isEqualTo( preAllocationSize );
    }

    private DatabaseManagementService dbms( Consumer<DatabaseManagementServiceBuilder> configuration )
    {
        configuration.accept( builder );
        dbms = builder.build();
        return dbms;
    }

    private void truncate( Path file, int size ) throws IOException
    {
        try ( StoreChannel storeChannel = fs.write( file ) )
        {
            storeChannel.truncate( size );
        }
    }

    private LogFiles buildLogFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        return LogFilesBuilder
                .logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fs )
                .build();
    }

    private long getTxLogFileSize() throws IOException
    {
        return fs.getFileSize( logFiles.getLogFile().getHighestLogFile() );
    }
}
