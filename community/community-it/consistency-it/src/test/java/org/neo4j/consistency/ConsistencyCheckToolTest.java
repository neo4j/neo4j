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
package org.neo4j.consistency;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.TimeZone;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckTool.ToolFailureException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class ConsistencyCheckToolTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void runsConsistencyCheck() throws Exception
    {
        // given
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        String[] args = {databaseLayout.databaseDirectory().getAbsolutePath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );

        // when
        runConsistencyCheckToolWith( service, args );

        // then
        verify( service ).runFullConsistencyCheck( any( DatabaseLayout.class ), any( Config.class ),
                any( ProgressMonitorFactory.class ), any( LogProvider.class ), any( FileSystemAbstraction.class ),
                anyBoolean(), any( ConsistencyFlags.class ) );
    }

    @Test
    void consistencyCheckerLogUseSystemTimezoneIfConfigurable() throws Exception
    {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try
        {
            ConsistencyCheckService service = mock( ConsistencyCheckService.class );
            Mockito.when( service.runFullConsistencyCheck( any( DatabaseLayout.class ), any( Config.class ),
                    any( ProgressMonitorFactory.class ), any( LogProvider.class ), any( FileSystemAbstraction.class ),
                    eq( false ), any( ConsistencyFlags.class ) ) )
                    .then( invocationOnMock ->
                    {
                        LogProvider provider = invocationOnMock.getArgument( 3 );
                        provider.getLog( "test" ).info( "testMessage" );
                        return ConsistencyCheckService.Result.success( new File( StringUtils.EMPTY ) );
                    } );
            File storeDir = testDirectory.directory();
            File configFile = testDirectory.file( Config.DEFAULT_CONFIG_FILE_NAME );
            Properties properties = new Properties();
            properties.setProperty( GraphDatabaseSettings.db_timezone.name(), LogTimeZone.SYSTEM.name() );
            properties.store( new FileWriter( configFile ), null );
            String[] args = {storeDir.getPath(), "-config", configFile.getPath()};

            checkLogRecordTimeZone( service, args, 5, "+0500" );
            checkLogRecordTimeZone( service, args, -5, "-0500" );
        }
        finally
        {
            TimeZone.setDefault( defaultTimeZone );
        }
    }

    @Test
    void appliesDefaultTuningConfigurationForConsistencyChecker() throws Exception
    {
        // given
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        String[] args = {databaseLayout.databaseDirectory().getAbsolutePath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );

        // when
        runConsistencyCheckToolWith( service, args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).runFullConsistencyCheck( any( DatabaseLayout.class ), config.capture(),
                any( ProgressMonitorFactory.class ), any( LogProvider.class ), any( FileSystemAbstraction.class ),
                anyBoolean(), any( ConsistencyFlags.class ) );
        assertFalse( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
    }

    @Test
    void passesOnConfigurationIfProvided() throws Exception
    {
        // given
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File configFile = testDirectory.file( Config.DEFAULT_CONFIG_FILE_NAME );
        Properties properties = new Properties();
        properties.setProperty( ConsistencyCheckSettings.consistency_check_property_owners.name(), "true" );
        properties.store( new FileWriter( configFile ), null );

        String[] args = {databaseLayout.databaseDirectory().getAbsolutePath(), "-config", configFile.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );

        // when
        runConsistencyCheckToolWith( service, args );

        // then
        ArgumentCaptor<Config> config = ArgumentCaptor.forClass( Config.class );
        verify( service ).runFullConsistencyCheck( any(), config.capture(),
                any( ProgressMonitorFactory.class ), any( LogProvider.class ), any( FileSystemAbstraction.class ),
                anyBoolean(), any(ConsistencyFlags.class) );
        assertTrue( config.getValue().get( ConsistencyCheckSettings.consistency_check_property_owners ) );
    }

    @Test
    void exitWithFailureIndicatingCorrectUsageIfNoArgumentsSupplied()
    {
        // given
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );
        String[] args = {};

        ToolFailureException toolException = assertThrows( ToolFailureException.class, () -> runConsistencyCheckToolWith( service, args ) );
        assertThat( toolException.getMessage(), containsString( "USAGE:" ) );
    }

    @Test
    void exitWithFailureIfConfigSpecifiedButConfigFileDoesNotExist()
    {
        // given
        File configFile = testDirectory.file( "nonexistent_file" );
        String[] args = {testDirectory.directory().getPath(), "-config", configFile.getPath()};
        ConsistencyCheckService service = mock( ConsistencyCheckService.class );

        ToolFailureException toolException = assertThrows( ToolFailureException.class, () -> runConsistencyCheckToolWith( service, args ) );
        assertThat( toolException.getMessage(), containsString( "Could not read configuration file" ) );
        assertThat( toolException.getCause().getMessage(), containsString( "does not exist" ) );

        verifyZeroInteractions( service );
    }

    @Test
    void failWhenStoreWasNonCleanlyShutdown()
    {
        assertThrows( ToolFailureException.class, () -> {
            createGraphDbAndKillIt( Config.defaults() );
            runConsistencyCheckToolWith( fs, testDirectory.databaseDir().getAbsolutePath() );
        } );
    }

    @Test
    void failOnNotCleanlyShutdownStoreWithLogsInCustomAbsoluteLocation()
    {
        assertThrows( ToolFailureException.class, () ->
        {
            File customConfigFile = testDirectory.file( "customConfig" );
            Config customConfig = Config.defaults();
            createGraphDbAndKillIt( customConfig );
            MapUtil.store( customConfig.getRaw(), fs.openAsOutputStream( customConfigFile, false ) );
            String[] args = {testDirectory.databaseDir().getPath(), "-config", customConfigFile.getPath()};

            runConsistencyCheckToolWith( fs, args );
        } );
    }

    private static void checkLogRecordTimeZone( ConsistencyCheckService service, String[] args, int hoursShift, String timeZoneSuffix )
            throws ToolFailureException, IOException
    {
        TimeZone.setDefault( TimeZone.getTimeZone( ZoneOffset.ofHours( hoursShift ) ) );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( outputStream );
        runConsistencyCheckToolWith( service, printStream, args );
        String logLine = readLogLine( outputStream );
        assertTrue( logLine.contains( timeZoneSuffix ), logLine );
    }

    private static String readLogLine( ByteArrayOutputStream outputStream ) throws IOException
    {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( byteArrayInputStream ) );
        return bufferedReader.readLine();
    }

    private void createGraphDbAndKillIt( Config config ) throws IOException
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( fs )
                .setConfigRaw( config.getRaw()  ).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "FOO" ) );
            db.createNode( label( "BAR" ) );
            tx.success();
        }
        File logFilesDirectory = getLogFilesDirectory( db );

        File tempLogsDirectory = testDirectory.directory( "logs-temp" );
        File tempStoreDirectory = testDirectory.directory( "tempDirectory" );

        createStoreCopy( logFilesDirectory, tempLogsDirectory, tempStoreDirectory );

        managementService.shutdown();

        restoreStoreCopy( logFilesDirectory, tempLogsDirectory, tempStoreDirectory );
    }

    private static File getLogFilesDirectory( GraphDatabaseAPI db )
    {
        LogFiles logFiles = db.getDependencyResolver().resolveDependency( LogFiles.class );
        return logFiles.logFilesDirectory();
    }

    private void restoreStoreCopy( File logFilesDirectory, File tempLogsDirectory, File tempStoreDirectory ) throws IOException
    {
        fs.copyRecursively( tempLogsDirectory, logFilesDirectory );
        fs.copyRecursively( tempStoreDirectory, testDirectory.databaseDir() );
    }

    private void createStoreCopy( File logFilesDirectory, File tempLogsDirectory, File tempStoreDirectory ) throws IOException
    {
        fs.copyRecursively( logFilesDirectory, tempLogsDirectory );
        fs.copyRecursively( testDirectory.databaseDir(), tempStoreDirectory );
    }

    private static void runConsistencyCheckToolWith( FileSystemAbstraction fileSystem, String... args )
            throws ToolFailureException
    {
        new ConsistencyCheckTool( mock( ConsistencyCheckService.class ), fileSystem, mock( PrintStream.class),
                mock( PrintStream.class ) ).run( args );
    }

    private static void runConsistencyCheckToolWith( ConsistencyCheckService consistencyCheckService, String... args ) throws ToolFailureException, IOException
    {
        runConsistencyCheckToolWith( consistencyCheckService, mock( PrintStream.class ), args );
    }

    private static void runConsistencyCheckToolWith( ConsistencyCheckService consistencyCheckService, PrintStream printStream, String... args )
            throws ToolFailureException, IOException
    {
        try ( FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction() )
        {
            new ConsistencyCheckTool( consistencyCheckService, fileSystemAbstraction, printStream, printStream ).run( args );
        }
    }
}
