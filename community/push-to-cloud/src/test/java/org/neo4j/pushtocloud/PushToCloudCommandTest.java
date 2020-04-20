/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.pushtocloud;

import org.apache.commons.io.output.NullOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.pushtocloud.PushToCloudCommand.Copier;
import org.neo4j.pushtocloud.PushToCloudCommand.DumpCreator;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ExtendWith( TestDirectorySupportExtension.class )
class PushToCloudCommandTest
{
    private static final String SOME_EXAMPLE_BOLT_URI = "bolt+routing://database_id.databases.neo4j.io";
    public static final String DBNAME = "neo4j";

    @Inject
    TestDirectory directory;
    private Path homeDir;
    private Path dump;
    private ExecutionContext ctx;

    @BeforeAll
    void setUp() throws IOException
    {
        homeDir = directory.directory( "home-dir" ).toPath();
        Path configDir = directory.directory( "config-dir" ).toPath();
        Path configFile = configDir.resolve( "neo4j.conf" );
        Files.createFile( configFile );
        PrintStream nullOutputStream = new PrintStream( NullOutputStream.nullOutputStream() );
        ctx = new ExecutionContext( homeDir, configDir, nullOutputStream, nullOutputStream, directory.getFileSystem() );
        createDbAndDump();
    }

    private void createDbAndDump()
    {
        Config config = Config.newBuilder()
                              .set( GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath() )
                              .set( default_database, DBNAME )
                              .build();
        DatabaseLayout databaseLayout = DatabaseLayout.of( config );

        Neo4jLayout neo4jLayout = databaseLayout.getNeo4jLayout();
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( neo4jLayout.homeDirectory() )
                        .setConfig( config )
                        .build();
        managementService.database( databaseLayout.getDatabaseName() );
        managementService.shutdown();

        dump = directory.file( "some-archive.dump" ).toPath();
        new RealDumpCreator( ctx ).dumpDatabase( DBNAME, dump );
    }

    @Test
    public void shouldReadUsernameAndPasswordFromUserInput() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .console( PushToCloudConsole.fakeConsole( username, password ) )
                .build();

        // when
        String[] args = {
                "--dump", dump.toString(),
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine( command ).execute( args );

        // then
        verify( targetCommunicator ).authenticate( anyBoolean(), any(), eq( username ), eq( password.toCharArray() ), anyBoolean() );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), any(), eq( false ), any() );
    }

    //
    @Test
    void shouldAcceptDumpAsSource() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        PushToCloudCommand command = command().copier( targetCommunicator ).build();

        // when
        Path dump = this.dump;
        String[] args = {"--dump", dump.toString(),
                         "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine( command ).execute( args );

        // then
        verify( targetCommunicator ).checkSize( anyBoolean(), any(), anyLong(), any() );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), eq( dump ), eq( false ), any() );
    }

    @Test
    public void shouldAcceptDatabaseNameAsSource() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        DumpCreator dumpCreator = mock( DumpCreator.class );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .dumpCreator( dumpCreator )
                .build();

        // when
        String databaseName = DBNAME;
        String[] args = {
                "--database", databaseName,
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI
        };
        new CommandLine( command ).execute( args );

        // then
        verify( dumpCreator ).dumpDatabase( eq( databaseName ), any() );
        verify( targetCommunicator ).checkSize( anyBoolean(), any(), anyLong(), any() );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), any(), eq( true ), any() );
    }

    @Test
    public void shouldAcceptDatabaseNameAsSourceUsingGivenDumpTarget() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        DumpCreator dumpCreator = mock( DumpCreator.class );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .dumpCreator( dumpCreator )
                .build();

        // when
        String databaseName = DBNAME;
        Path dumpFile = directory.file( "some-dump-file" ).toPath();
        String[] args =
                {"--database", databaseName,
                 "--dump-to", dumpFile.toString(),
                 "--bolt-uri", SOME_EXAMPLE_BOLT_URI};

        new CommandLine( command ).execute( args );

        // then
        verify( dumpCreator ).dumpDatabase( databaseName, dumpFile );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), any(), eq( true ), any() );
    }

    @Test
    public void shouldFailOnDatabaseNameAsSourceUsingExistingDumpTarget() throws IOException, CommandFailedException
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        DumpCreator dumpCreator = mock( DumpCreator.class );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .dumpCreator( dumpCreator )
                .build();

        // when
        String databaseName = DBNAME;
        Path dumpFile = directory.file( "some-dump-file-that-exists" ).toPath();
        Files.write( dumpFile, "some data".getBytes() );
        String[] args =
                {"--database", databaseName,
                 "--dump-to", dumpFile.toString(),
                 "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        int returnValue = new CommandLine( command ).execute( args );

        assertNotEquals( 0, returnValue, "Expected command to fail" );
    }

    @Test
    public void shouldNotAcceptBothDumpAndDatabaseNameAsSource() throws IOException, CommandFailedException
    {
        // given
        PushToCloudCommand command = command().copier( mockedTargetCommunicator() ).build();

        // when
        String[] args = {
                "--dump", directory.file( "some-dump-file" ).toPath().toString(),
                "--database", DBNAME,
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        int returnValue = new CommandLine( command ).execute( args );
        assertNotEquals( 0, returnValue, "Expected command to fail" );
    }

    @Test
    public void shouldAcceptPasswordViaArgAndPromptForUsername() throws IOException, CommandFailedException
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";

        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .console( PushToCloudConsole.fakeConsole( username, "tomte" ) )
                .build();

        Path dump = this.dump;
        // when
        String[] args = {
                "--dump", dump.toString(),
                "--password", "pass",
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine( command ).execute( args );
        verify( targetCommunicator ).authenticate( anyBoolean(), anyString(), eq( "neo4j" ), eq( "pass".toCharArray() ), anyBoolean() );
    }

    @Test
    public void shouldAcceptPasswordViaEnvAndPromptForUsername() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";

        PushToCloudCommand command = command().copier( targetCommunicator ).console( PushToCloudConsole.fakeConsole( username, "tomte" ) ).build();

        Path dump = this.dump;
        // when
        String[] args = {
                "--dump", dump.toString(),
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI
        };
        var environment = Map.of( "NEO4J_USERNAME", "", "NEO4J_PASSWORD", "pass" );
        new CommandLine( command ).setResourceBundle( new MapResourceBundle( environment ) ).execute( args );

        verify( targetCommunicator ).authenticate( anyBoolean(), anyString(), eq( "neo4j" ), eq( "pass".toCharArray() ), anyBoolean() );
    }

    @Test
    public void shouldAcceptUsernameViaArgAndPromptForPassword() throws IOException, CommandFailedException
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .console( PushToCloudConsole.fakeConsole( username, password ) )
                .build();

        Path dump = this.dump;
        // when
        String[] args = {
                "--dump", dump.toString(),
                "--username", "user",
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};

        new CommandLine( command ).execute( args );
        assertTrue( dump.toFile().exists() );
        verify( targetCommunicator ).authenticate( anyBoolean(), anyString(), eq( "user" ), eq( "abc".toCharArray() ), anyBoolean() );
    }

    @Test
    public void shouldAcceptUsernameViaEnvAndPromptForPassword() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .console( PushToCloudConsole.fakeConsole( username, password ) )
                .build();

        Path dump = this.dump;
        // when
        String[] args = {
                "--dump", dump.toString(),
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};

        var environment = Map.of( "NEO4J_USERNAME", "user", "NEO4J_PASSWORD", "" );
        new CommandLine( command ).setResourceBundle( new MapResourceBundle( environment ) ).execute( args );
        assertTrue( dump.toFile().exists() );
        verify( targetCommunicator ).authenticate( anyBoolean(), anyString(), eq( "user" ), eq( "abc".toCharArray() ), anyBoolean() );
    }

    @Test
    public void shouldAcceptOnlyUsernameAndPasswordFromCli() throws IOException, CommandFailedException
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .console( PushToCloudConsole.fakeConsole( username, password ) )
                .build();

        // when
        String[] args = {
                "--dump", dump.toString(),
                "--username", "neo4jcli",
                "--password", "passcli",
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine( command ).execute( args );

        verify( targetCommunicator ).authenticate( anyBoolean(), anyString(), eq( "neo4jcli" ), eq( "passcli".toCharArray() ), anyBoolean() );
    }

    @Test
    public void shouldAcceptOnlyUsernameAndPasswordFromEnv() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .console( PushToCloudConsole.fakeConsole( username, password ) )
                .build();

        // when
        String[] args = {
                "--dump", dump.toString(),
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        var environment = Map.of( "NEO4J_USERNAME", "neo4jenv", "NEO4J_PASSWORD", "passenv" );
        new CommandLine( command ).setResourceBundle( new MapResourceBundle( environment ) ).execute( args );

        verify( targetCommunicator ).authenticate( anyBoolean(), anyString(), eq( "neo4jenv" ), eq( "passenv".toCharArray() ), anyBoolean() );
    }

    @Test
    public void shouldChooseToDumpDefaultDatabaseIfNeitherDumpNorDatabaseIsGiven() throws IOException, CommandFailedException
    {
        // given
        DumpCreator dumpCreator = mock( DumpCreator.class );
        Copier copier = mock( Copier.class );
        PushToCloudCommand command = command()
                .dumpCreator( dumpCreator )
                .copier( copier )
                .build();

        // when
        String[] args = {"--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine( command ).execute( args );

        // then
        verify( dumpCreator ).dumpDatabase( eq( DEFAULT_DATABASE_NAME ), any() );
        verify( copier ).copy( anyBoolean(), any(), any(), any(), eq( true ), any() );
    }

    @Test
    public void shouldFailOnDumpPointingToMissingFile() throws IOException, CommandFailedException
    {
        // given
        PushToCloudCommand command = command().copier( mockedTargetCommunicator() ).build();

        // when
        File dumpFile = directory.file( "some-dump-file" );
        String[] args = {
                "--dump", dumpFile.getAbsolutePath(),
                "--bolt-uri", SOME_EXAMPLE_BOLT_URI};
        int returnValue = new CommandLine( command ).execute( args );
        assertNotEquals( 0, returnValue, "Expected command to fail" );
    }

    // TODO: 2019-08-07 shouldFailOnDumpPointingToInvalidDumpFile

    @Test
    public void shouldRecognizeBothEnvironmentAndDatabaseIdFromBoltURI() throws IOException, CommandFailedException
    {
        // given
        Copier copier = mock( Copier.class );
        PushToCloudCommand command = command().copier( copier ).build();

        // when
        String[] args = {
                "--dump", dump.toString(),
                "--bolt-uri", "bolt+routing://mydbid-testenvironment.databases.neo4j.io"
        };
        new CommandLine( command ).execute( args );
        // then
        verify( copier ).copy( anyBoolean(), eq( "https://console-testenvironment.neo4j.io/v1/databases/mydbid" ),
                               eq( "bolt+routing://mydbid-testenvironment.databases.neo4j.io" ), any(), eq( false ), any() );
    }

    @Test
    public void shouldRecognizeDatabaseIdFromBoltURI() throws IOException, CommandFailedException
    {
        // given
        Copier copier = mock( Copier.class );
        PushToCloudCommand command = command().copier( copier ).build();

        // when
        String[] args = {
                "--dump", dump.toString(),
                "--bolt-uri", "bolt+routing://mydbid.databases.neo4j.io"};
        new CommandLine( command ).execute( args );
        // then
        verify( copier ).copy( anyBoolean(), eq( "https://console.neo4j.io/v1/databases/mydbid" ),
                               eq( "bolt+routing://mydbid.databases.neo4j.io" ), any(), eq( false ), any() );
    }

    @Test
    public void shouldAuthenticateBeforeDumping() throws CommandFailedException, IOException
    {
        // given
        Copier copier = mockedTargetCommunicator();
        DumpCreator dumper = mock( DumpCreator.class );
        PushToCloudCommand command = command().copier( copier ).dumpCreator( dumper ).build();

        // when
        String[] args = {"--bolt-uri", "bolt+routing://mydbid.databases.neo4j.io"};
        new CommandLine( command ).execute( args );

        // then
        InOrder inOrder = inOrder( copier, dumper );
        inOrder.verify( copier ).authenticate( anyBoolean(), anyString(), anyString(), any(), eq( false ) );
        inOrder.verify( dumper ).dumpDatabase( anyString(), any() );
        inOrder.verify( copier ).copy( anyBoolean(), anyString(), eq( "bolt+routing://mydbid.databases.neo4j.io" ), any(),
                                       eq( true ), anyString() );
    }

    private Copier mockedTargetCommunicator() throws CommandFailedException
    {
        Copier copier = mock( Copier.class );
        when( copier.authenticate( anyBoolean(), any(), any(), any(), anyBoolean() ) ).thenReturn( "abc" );
        return copier;
    }

    private Builder command()
    {
        return new Builder();
    }

    private static class MapResourceBundle extends ResourceBundle
    {
        private final Map<String,String> entries;

        MapResourceBundle( Map<String,String> entries )
        {
            requireNonNull( entries );
            this.entries = entries;
        }

        @Override
        protected Object handleGetObject( String key )
        {
            requireNonNull( key );
            return entries.get( key );
        }

        @Override
        public Enumeration<String> getKeys()
        {
            return Collections.enumeration( entries.keySet() );
        }
    }

    private class Builder
    {
        private final Map<Setting<?>,String> settings = new HashMap<>();
        private ExecutionContext executionContext = ctx;
        private DumpCreator dumpCreator = mock( DumpCreator.class );
        private Copier targetCommunicator;
        private PushToCloudConsole console = PushToCloudConsole.fakeConsole( "tomte", "tomtar" );

        Builder copier( Copier targetCommunicator )
        {
            this.targetCommunicator = targetCommunicator;
            return this;
        }

        Builder dumpCreator( DumpCreator dumpCreator )
        {
            this.dumpCreator = dumpCreator;
            return this;
        }

        Builder console( PushToCloudConsole console )
        {
            this.console = console;
            return this;
        }

        PushToCloudCommand build() throws IOException
        {
            return new PushToCloudCommand( executionContext, targetCommunicator, dumpCreator, console );
        }
    }
}
