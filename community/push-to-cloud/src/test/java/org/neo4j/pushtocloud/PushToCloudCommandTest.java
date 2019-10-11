/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.common.Database;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.pushtocloud.PushToCloudCommand.Copier;
import org.neo4j.pushtocloud.PushToCloudCommand.DumpCreator;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.pushtocloud.PushToCloudCommand.ARG_BOLT_URI;
import static org.neo4j.pushtocloud.PushToCloudCommand.ARG_DATABASE;
import static org.neo4j.pushtocloud.PushToCloudCommand.ARG_DUMP;
import static org.neo4j.pushtocloud.PushToCloudCommand.ARG_DUMP_TO;
import static org.neo4j.pushtocloud.PushToCloudCommand.ARG_PASSWORD;
import static org.neo4j.pushtocloud.PushToCloudCommand.ARG_USERNAME;
import static org.neo4j.pushtocloud.PushToCloudCommand.ARG_OVERWRITE;

public class PushToCloudCommandTest
{
    private static final String SOME_EXAMPLE_BOLT_URI = "bolt+routing://database_id.databases.neo4j.io";

    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void shouldReadUsernameAndPasswordFromUserInput() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        char[] password = {'a', 'b', 'c'};
        OutsideWorld outsideWorld = new ControlledOutsideWorld( new DefaultFileSystemAbstraction() )
                .withPromptResponse( username )
                .withPasswordResponse( password );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .outsideWorld( outsideWorld )
                .build();

        // when
        command.execute( array(
                arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );

        // then
        verify( targetCommunicator ).authenticate( anyBoolean(), any(), eq( username ), eq( password ), anyBoolean() );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), any(), any() );
    }

    @Test
    public void shouldAcceptConfirmationViaCommandLine() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        char[] password = {'a', 'b', 'c'};
        OutsideWorld outsideWorld = new ControlledOutsideWorld( new DefaultFileSystemAbstraction() )
                .withPromptResponse( username )
                .withPasswordResponse( password );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .outsideWorld( outsideWorld )
                .build();

        // when
        command.execute( array(
                arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ),
                arg( ARG_OVERWRITE, "true" ) ) );

        // then
        verify( targetCommunicator ).authenticate( anyBoolean(), any(), eq( username ), eq( password ), anyBoolean() );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), any(), any() );
    }

    @Test
    public void shouldAcceptDumpAsSource() throws Exception
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        PushToCloudCommand command = command().copier( targetCommunicator ).build();

        // when
        Path dump = createSimpleDatabaseDump();
        command.execute( array(
                arg( ARG_DUMP, dump.toString() ),
                arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );

        // then
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), eq( dump ), any() );
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
        String databaseName = "neo4j";
        command.execute( array(
                arg( ARG_DATABASE, databaseName ),
                arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );

        // then
        verify( dumpCreator ).dumpDatabase( eq( databaseName ), any() );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), any(), any() );
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
        String databaseName = "neo4j";
        Path dumpFile = directory.file( "some-dump-file" ).toPath();
        command.execute( array(
                arg( ARG_DATABASE, databaseName ),
                arg( ARG_DUMP_TO, dumpFile.toString() ),
                arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );

        // then
        verify( dumpCreator ).dumpDatabase( databaseName, dumpFile );
        verify( targetCommunicator ).copy( anyBoolean(), any(), any(), any(), any() );
    }

    @Test
    public void shouldFailOnDatabaseNameAsSourceUsingExistingDumpTarget() throws IOException, IncorrectUsage, CommandFailed
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        DumpCreator dumpCreator = mock( DumpCreator.class );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .dumpCreator( dumpCreator )
                .build();

        // when
        String databaseName = "neo4j";
        Path dumpFile = directory.file( "some-dump-file" ).toPath();
        Files.write( dumpFile, "some data".getBytes() );
        try
        {
            command.execute( array(
                    arg( ARG_DATABASE, databaseName ),
                    arg( ARG_DUMP_TO, dumpFile.toString() ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( CommandFailed commandFailed )
        {
            // then
            assertThat( commandFailed.getMessage(), containsString( "already exists" ) );
        }
    }

    @Test
    public void shouldNotAcceptBothDumpAndDatabaseNameAsSource() throws IOException, CommandFailed
    {
        // given
        PushToCloudCommand command = command().copier( mockedTargetCommunicator() ).build();

        // when
        try
        {
            command.execute( array(
                    arg( ARG_DUMP, directory.file( "some-dump-file" ).toPath().toString() ),
                    arg( ARG_DATABASE, "neo4j" ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( IncorrectUsage incorrectUsage )
        {
            // then good
        }
    }

    @Test
    public void shouldNotAcceptOnlyUsernameOrPasswordFromArgument() throws IOException, CommandFailed
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        char[] password = {'a', 'b', 'c'};
        OutsideWorld outsideWorld = new ControlledOutsideWorld( new DefaultFileSystemAbstraction() )
                .withPromptResponse( username )
                .withPasswordResponse( password );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .outsideWorld( outsideWorld )
                .build();

        // when
        try
        {
            command.execute( array(
                    arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                    arg( ARG_USERNAME, "user" ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( IncorrectUsage incorrectUsage )
        {
            // then good
        }

        try
        {
            command.execute( array(
                    arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                    arg( ARG_PASSWORD, "pass" ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( IncorrectUsage incorrectUsage )
        {
            // then good
        }
    }

    @Test
    public void shouldNotAcceptOnlyUsernameOrPasswordFromEnvVar() throws IOException, CommandFailed
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        char[] password = {'a', 'b', 'c'};
        OutsideWorld outsideWorld = new ControlledOutsideWorld( new DefaultFileSystemAbstraction() )
                .withPromptResponse( username )
                .withPasswordResponse( password );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .outsideWorld( outsideWorld )
                .build();

        // when
        try
        {
            environmentVariables.set("NEO4J_USERNAME", "neo4j");
            environmentVariables.set("NEO4J_PASSWORD", null);
            command.execute( array(
                    arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( IncorrectUsage incorrectUsage )
        {
            // then good
        }

        try
        {
            environmentVariables.set("NEO4J_USERNAME", null);
            environmentVariables.set("NEO4J_PASSWORD", "pass");
            command.execute( array(
                    arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( IncorrectUsage incorrectUsage )
        {
            // then good
        }
    }

    @Test
    public void shouldNotAcceptOnlyUsernameAndPasswordFromEnvAndCli() throws IOException, CommandFailed
    {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        char[] password = {'a', 'b', 'c'};
        OutsideWorld outsideWorld = new ControlledOutsideWorld( new DefaultFileSystemAbstraction() )
                .withPromptResponse( username )
                .withPasswordResponse( password );
        PushToCloudCommand command = command()
                .copier( targetCommunicator )
                .outsideWorld( outsideWorld )
                .build();

        // when
        try
        {
            environmentVariables.set("NEO4J_USERNAME", "neo4j");
            environmentVariables.set("NEO4J_PASSWORD", "pass");
            command.execute( array(
                    arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                    arg( ARG_USERNAME, "neo4j" ),
                    arg( ARG_PASSWORD, "pass" ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( IncorrectUsage incorrectUsage )
        {
            // then good
        }

    }

    @Test
    public void shouldChooseToDumpDefaultDatabaseIfNeitherDumpNorDatabaseIsGiven() throws IOException, CommandFailed, IncorrectUsage
    {
        // given
        DumpCreator dumpCreator = mock( DumpCreator.class );
        Copier copier = mock( Copier.class );
        PushToCloudCommand command = command().dumpCreator( dumpCreator ).copier( copier ).build();

        // when
        command.execute( array(
                arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );

        // then
        String defaultDatabase = new Database().defaultValue();
        verify( dumpCreator ).dumpDatabase( eq( defaultDatabase ), any() );
        verify( copier ).copy( anyBoolean(), any(), any(), any(), any() );
    }

    @Test
    public void shouldFailOnDumpPointingToMissingFile() throws IOException, IncorrectUsage, CommandFailed
    {
        // given
        PushToCloudCommand command = command().copier( mockedTargetCommunicator() ).build();

        // when
        try
        {
            File dumpFile = directory.file( "some-dump-file" );
            command.execute( array(
                    arg( ARG_DUMP, dumpFile.getAbsolutePath() ),
                    arg( ARG_BOLT_URI, SOME_EXAMPLE_BOLT_URI ) ) );
            fail( "Should have failed" );
        }
        catch ( CommandFailed commandFailed )
        {
            // then good
        }
    }

    // TODO: 2019-08-07 shouldFailOnDumpPointingToInvalidDumpFile

    @Test
    public void shouldRecognizeBothEnvironmentAndDatabaseIdFromBoltURI() throws IOException, CommandFailed, IncorrectUsage
    {
        // given
        Copier copier = mock( Copier.class );
        PushToCloudCommand command = command().copier( copier ).build();

        // when
        command.execute( array(
                arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                arg( ARG_BOLT_URI, "bolt+routing://mydbid-testenvironment.databases.neo4j.io" ) ) );

        // then
        verify( copier ).copy( anyBoolean(), eq( "https://console-testenvironment.neo4j.io/v1/databases/mydbid" ),
                eq( "bolt+routing://mydbid-testenvironment.databases.neo4j.io" ), any(), any() );
    }

    @Test
    public void shouldRecognizeDatabaseIdFromBoltURI() throws IOException, CommandFailed, IncorrectUsage
    {
        // given
        Copier copier = mock( Copier.class );
        PushToCloudCommand command = command().copier( copier ).build();

        // when
        command.execute( array(
                arg( ARG_DUMP, createSimpleDatabaseDump().toString() ),
                arg( ARG_BOLT_URI, "bolt+routing://mydbid.databases.neo4j.io" ) ) );

        // then
        verify( copier ).copy( anyBoolean(), eq( "https://console.neo4j.io/v1/databases/mydbid" ),
                eq( "bolt+routing://mydbid.databases.neo4j.io" ), any(), any() );
    }

    @Test
    public void shouldAuthenticateBeforeDumping() throws CommandFailed, IOException, IncorrectUsage
    {
        // given
        Copier copier = mockedTargetCommunicator();
        DumpCreator dumper = mock( DumpCreator.class );
        PushToCloudCommand command = command().copier( copier ).dumpCreator( dumper ).build();

        // when
        command.execute( array( arg( ARG_BOLT_URI, "bolt+routing://mydbid.databases.neo4j.io" ) ) );

        // then
        InOrder inOrder = inOrder( copier, dumper );
        inOrder.verify( copier ).authenticate( anyBoolean(), anyString(), anyString(), any(), anyBoolean() );
        inOrder.verify( dumper ).dumpDatabase( anyString(), any() );
        inOrder.verify( copier ).copy( anyBoolean(), anyString(), eq( "bolt+routing://mydbid.databases.neo4j.io" ), any(), anyString() );
    }

    private Copier mockedTargetCommunicator() throws CommandFailed
    {
        Copier copier = mock( Copier.class );
        when( copier.authenticate( anyBoolean(), any(), any(), any(), anyBoolean() ) ).thenReturn( "abc" );
        return copier;
    }

    private Path createSimpleDatabaseDump() throws IOException
    {
        Path dump = directory.file( "dump" ).toPath();
        Files.write( dump, "some data".getBytes() );
        return dump;
    }

    private String arg( String key, String value )
    {
        return format( "--%s=%s", key, value );
    }

    private Builder command()
    {
        return new Builder();
    }

    private class Builder
    {
        private Path homeDir = directory.directory().toPath();
        private Path configDir = directory.directory( "conf" ).toPath();
        private OutsideWorld outsideWorld = new ControlledOutsideWorld( new DefaultFileSystemAbstraction() );
        private DumpCreator dumpCreator = mock( DumpCreator.class );
        private Copier targetCommunicator;
        private final Map<Setting<?>,String> settings = new HashMap<>();

        Builder config( Setting<?> setting, String value )
        {
            settings.put( setting, value );
            return this;
        }

        Builder copier( Copier targetCommunicator )
        {
            this.targetCommunicator = targetCommunicator;
            return this;
        }

        Builder outsideWorld( OutsideWorld outsideWorld )
        {
            this.outsideWorld = outsideWorld;
            return this;
        }

        Builder dumpCreator( DumpCreator dumpCreator )
        {
            this.dumpCreator = dumpCreator;
            return this;
        }

        PushToCloudCommand build() throws IOException
        {
            return new PushToCloudCommand( homeDir, buildConfig(), outsideWorld, targetCommunicator, dumpCreator );
        }

        private Path buildConfig() throws IOException
        {
            StringBuilder configFileContents = new StringBuilder();
            settings.forEach( ( key, value ) -> configFileContents.append( format( "%s=%s%n", key.name(), value ) ) );
            Path configFile = configDir.resolve( "neo4j.conf" );
            Files.write( configFile, configFileContents.toString().getBytes() );
            return configFile;
        }
    }
}
