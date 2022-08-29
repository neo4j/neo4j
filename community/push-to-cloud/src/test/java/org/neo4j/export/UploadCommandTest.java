/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.export;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.internal.helpers.collection.Iterators.array;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.ResourceBundle;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.DumpCommandProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.export.UploadCommand.Copier;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestDirectorySupportExtension.class)
class UploadCommandTest {
    private static final String SOME_EXAMPLE_BOLT_URI = "bolt+routing://database_id.databases.neo4j.io";
    public static final String DBNAME = "neo4j";

    @Inject
    TestDirectory directory;

    private Path homeDir;
    private Path dump;
    private ExecutionContext ctx;
    private Path dumpDir;

    @BeforeAll
    void setUp() throws IOException {
        homeDir = directory.directory("home-dir");
        Path configDir = directory.directory("config-dir");
        Path configFile = configDir.resolve("neo4j.conf");
        dumpDir = directory.directory("dumps");
        dump = dumpDir.resolve(DBNAME + ".dump");
        Files.createFile(configFile);
        PrintStream nullOutputStream = new PrintStream(NullOutputStream.nullOutputStream());
        ctx = new ExecutionContext(homeDir, configDir, nullOutputStream, nullOutputStream, directory.getFileSystem());
        createDbAndDump(dumpDir);
    }

    private void createDbAndDump(Path dumpDir) {
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath())
                .set(initial_default_database, DBNAME)
                .build();
        DatabaseLayout databaseLayout = DatabaseLayout.of(config);

        Neo4jLayout neo4jLayout = databaseLayout.getNeo4jLayout();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(
                        neo4jLayout.homeDirectory())
                .setConfig(config)
                .build();
        managementService.database(databaseLayout.getDatabaseName());
        managementService.shutdown();

        String[] args = array(DBNAME, "--to-path", dumpDir.toString());
        new CommandLine(new DumpCommandProvider().createCommand(ctx)).execute(args);
    }

    @Test
    public void shouldReadUsernameAndPasswordFromUserInput() {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        UploadCommand command = command()
                .copier(targetCommunicator)
                .console(PushToCloudConsole.fakeConsole(username, password))
                .build();

        // when
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine(command).execute(args);

        // then
        verify(targetCommunicator)
                .authenticate(anyBoolean(), any(), eq(username), eq(password.toCharArray()), anyBoolean());
        verify(targetCommunicator).copy(anyBoolean(), any(), any(), any(), eq(false), any());
    }

    @Test
    public void shouldUseNeo4jAsDefaultUsernameIfUserHitsEnter() {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        PushToCloudConsole console = mock(PushToCloudConsole.class);
        when(console.readLine(anyString(), anyString())).thenReturn("");
        String defaultUsername = "neo4j";
        String password = "super-secret-password";
        UploadCommand command =
                command().copier(targetCommunicator).console(console).build();

        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI, "--to-password", password
        };
        new CommandLine(command).execute(args);

        // then
        verify(console).readLine("%s", format("Neo4j aura username (default: %s):", defaultUsername));
        verify(targetCommunicator)
                .authenticate(anyBoolean(), any(), eq(defaultUsername), eq(password.toCharArray()), anyBoolean());
        verify(targetCommunicator).copy(anyBoolean(), any(), any(), any(), eq(false), any());
    }

    @Test
    public void shouldUseNeo4jAsDefaultUsernameIfStdinIndicatesEndOfFile() {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        PushToCloudConsole console = mock(PushToCloudConsole.class);
        when(console.readLine(anyString(), anyString())).thenReturn(null);
        String defaultUsername = "neo4j";
        String password = "super-secret-password";
        UploadCommand command =
                command().copier(targetCommunicator).console(console).build();

        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI, "--to-password", password
        };
        new CommandLine(command).execute(args);

        // then
        verify(targetCommunicator)
                .authenticate(anyBoolean(), any(), eq(defaultUsername), eq(password.toCharArray()), anyBoolean());
        verify(targetCommunicator).copy(anyBoolean(), any(), any(), any(), eq(false), any());
    }

    @Test
    void shouldAcceptDumpAsSource() {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        UploadCommand command = command().copier(targetCommunicator).build();

        // when
        UploadCommand.Uploader uploader = command.makeDumpUploader(dumpDir, DBNAME);
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine(command).execute(args);

        // then
        verify(targetCommunicator).checkSize(anyBoolean(), any(), anyLong(), any());
        verify(targetCommunicator).copy(anyBoolean(), any(), any(), eq(uploader.source), eq(false), any());
    }

    @Test
    public void shouldNotAcceptBothDumpAndDatabaseNameAsSource() throws CommandFailedException {
        // given
        UploadCommand command = command().copier(mockedTargetCommunicator()).build();

        // when
        String[] args = {
            "--dump", directory.file("some-dump-file").toString(),
            "--database", DBNAME,
            "--to-uri", SOME_EXAMPLE_BOLT_URI
        };
        int returnValue = new CommandLine(command).execute(args);
        assertNotEquals(0, returnValue, "Expected command to fail");
    }

    @Test
    public void shouldAcceptPasswordViaArgAndPromptForUsername() throws CommandFailedException {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";

        UploadCommand command = command()
                .copier(targetCommunicator)
                .console(PushToCloudConsole.fakeConsole(username, "tomte"))
                .build();

        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-password", "pass", "--to-uri", SOME_EXAMPLE_BOLT_URI
        };
        new CommandLine(command).execute(args);
        verify(targetCommunicator)
                .authenticate(anyBoolean(), anyString(), eq("neo4j"), eq("pass".toCharArray()), anyBoolean());
    }

    @Test
    public void shouldAcceptPasswordViaEnvAndPromptForUsername() {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";

        UploadCommand command = command()
                .copier(targetCommunicator)
                .console(PushToCloudConsole.fakeConsole(username, "tomte"))
                .build();

        // when
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        var environment = Map.of("NEO4J_USERNAME", "", "NEO4J_PASSWORD", "pass");
        new CommandLine(command)
                .setResourceBundle(new MapResourceBundle(environment))
                .execute(args);

        verify(targetCommunicator)
                .authenticate(anyBoolean(), anyString(), eq("neo4j"), eq("pass".toCharArray()), anyBoolean());
    }

    @Test
    public void shouldAcceptUsernameViaArgAndPromptForPassword() throws CommandFailedException {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        UploadCommand command = command()
                .copier(targetCommunicator)
                .console(PushToCloudConsole.fakeConsole(username, password))
                .build();

        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-user", "user", "--to-uri", SOME_EXAMPLE_BOLT_URI
        };

        new CommandLine(command).execute(args);
        assertTrue(Files.exists(dump));
        verify(targetCommunicator)
                .authenticate(anyBoolean(), anyString(), eq("user"), eq("abc".toCharArray()), anyBoolean());
    }

    @Test
    public void shouldAcceptUsernameViaEnvAndPromptForPassword() {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        UploadCommand command = command()
                .copier(targetCommunicator)
                .console(PushToCloudConsole.fakeConsole(username, password))
                .build();

        // when
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};

        var environment = Map.of("NEO4J_USERNAME", "user", "NEO4J_PASSWORD", "");
        new CommandLine(command)
                .setResourceBundle(new MapResourceBundle(environment))
                .execute(args);
        assertTrue(Files.exists(dump));
        verify(targetCommunicator)
                .authenticate(anyBoolean(), anyString(), eq("user"), eq("abc".toCharArray()), anyBoolean());
    }

    @Test
    public void shouldAcceptOnlyUsernameAndPasswordFromCli() throws CommandFailedException {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        UploadCommand command = command()
                .copier(targetCommunicator)
                .console(PushToCloudConsole.fakeConsole(username, password))
                .build();

        // when
        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toString(),
            "--to-user",
            "neo4jcli",
            "--to-password",
            "passcli",
            "--to-uri",
            SOME_EXAMPLE_BOLT_URI
        };
        new CommandLine(command).execute(args);

        verify(targetCommunicator)
                .authenticate(anyBoolean(), anyString(), eq("neo4jcli"), eq("passcli".toCharArray()), anyBoolean());
    }

    @Test
    public void shouldAcceptOnlyUsernameAndPasswordFromEnv() {
        // given
        Copier targetCommunicator = mockedTargetCommunicator();
        String username = "neo4j";
        String password = "abc";
        UploadCommand command = command()
                .copier(targetCommunicator)
                .console(PushToCloudConsole.fakeConsole(username, password))
                .build();

        // when
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        var environment = Map.of("NEO4J_USERNAME", "neo4jenv", "NEO4J_PASSWORD", "passenv");
        new CommandLine(command)
                .setResourceBundle(new MapResourceBundle(environment))
                .execute(args);

        verify(targetCommunicator)
                .authenticate(anyBoolean(), anyString(), eq("neo4jenv"), eq("passenv".toCharArray()), anyBoolean());
    }

    @Test
    public void shouldFailOnDumpPointingToMissingFile() throws CommandFailedException {
        // given
        UploadCommand command = command().copier(mockedTargetCommunicator()).build();

        // when
        String[] args = {"otherdbname", "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        int returnValue = new CommandLine(command).execute(args);
        assertNotEquals(0, returnValue, "Expected command to fail");
    }

    @Test
    public void shouldFailOnWrongDumpPath() throws CommandFailedException {
        // given
        UploadCommand command = command().copier(mockedTargetCommunicator()).build();

        // when
        Path dumpFile = directory.file("some-dump-file");
        String[] args = {DBNAME, "--from-path", dumpFile.toAbsolutePath().toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI
        };
        int returnValue = new CommandLine(command).execute(args);
        assertNotEquals(0, returnValue, "Expected command to fail");
    }

    // TODO: 2019-08-07 shouldFailOnDumpPointingToInvalidDumpFile

    @Test
    public void shouldRecognizeBothEnvironmentAndDatabaseIdFromBoltURI() throws CommandFailedException {
        // given
        Copier copier = mock(Copier.class);
        UploadCommand command = command().copier(copier).build();

        // when
        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toString(),
            "--to-uri",
            "bolt+routing://mydbid-testenvironment.databases.neo4j.io"
        };
        new CommandLine(command).execute(args);
        // then
        verify(copier)
                .copy(
                        anyBoolean(),
                        eq("https://console-testenvironment.neo4j.io/v1/databases/mydbid"),
                        eq("bolt+routing://mydbid-testenvironment.databases.neo4j.io"),
                        any(),
                        eq(false),
                        any());
    }

    @Test
    public void shouldRecognizeDatabaseIdFromBoltURI() throws CommandFailedException {
        // given
        Copier copier = mock(Copier.class);
        UploadCommand command = command().copier(copier).build();

        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-uri", "bolt+routing://mydbid.databases.neo4j.io"
        };
        new CommandLine(command).execute(args);
        // then
        verify(copier)
                .copy(
                        anyBoolean(),
                        eq("https://console.neo4j.io/v1/databases/mydbid"),
                        eq("bolt+routing://mydbid.databases.neo4j.io"),
                        any(),
                        eq(false),
                        any());
    }

    private Copier mockedTargetCommunicator() throws CommandFailedException {
        Copier copier = mock(Copier.class);
        when(copier.authenticate(anyBoolean(), any(), any(), any(), anyBoolean()))
                .thenReturn("abc");
        return copier;
    }

    private Builder command() {
        return new Builder();
    }

    private static class MapResourceBundle extends ResourceBundle {
        private final Map<String, String> entries;

        MapResourceBundle(Map<String, String> entries) {
            requireNonNull(entries);
            this.entries = entries;
        }

        @Override
        protected Object handleGetObject(String key) {
            requireNonNull(key);
            return entries.get(key);
        }

        @Override
        public Enumeration<String> getKeys() {
            return Collections.enumeration(entries.keySet());
        }
    }

    private class Builder {
        private final ExecutionContext executionContext = ctx;
        private Copier targetCommunicator;
        private PushToCloudConsole console = PushToCloudConsole.fakeConsole("tomte", "tomtar");

        Builder copier(Copier targetCommunicator) {
            this.targetCommunicator = targetCommunicator;
            return this;
        }

        Builder console(PushToCloudConsole console) {
            this.console = console;
            return this;
        }

        UploadCommand build() {
            return new UploadCommand(executionContext, targetCommunicator, console);
        }
    }
}
