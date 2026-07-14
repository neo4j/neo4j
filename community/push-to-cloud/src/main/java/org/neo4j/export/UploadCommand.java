/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.neo4j.export.DumpUploader.makeDumpUploader;

import java.nio.file.Path;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.aura.AuraClient;
import org.neo4j.export.aura.AuraConsole;
import org.neo4j.export.aura.AuraURLFactory;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "upload",
        description = "Push a local database to a Neo4j Aura instance. "
                + "The target location is a Neo4j Aura Bolt URI. If Neo4j Cloud username and password are not provided "
                + "either as a command option or as an environment variable, they will be requested interactively ")
public class UploadCommand extends AbstractAdminCommand {
    private static final String DEV_MODE_VAR_NAME = "P2C_DEV_MODE";
    private static final String ENV_NEO4J_USERNAME = "NEO4J_USERNAME";
    private static final String ENV_NEO4J_PASSWORD = "NEO4J_PASSWORD";
    private static final String TO_PASSWORD = "--to-password";
    private final PushToCloudCLI pushToCloudCLI;
    private final AuraClient.AuraClientBuilder clientBuilder;
    private final AuraURLFactory auraURLFactory;
    private final UploadURLFactory uploadURLFactory;

    @Parameters(
            paramLabel = "<database>",
            description = "Name of the database that should be uploaded. The name is used to select a file "
                    + "which is expected to be named <database>.dump or <database>.backup.",
            converter = Converters.DatabaseNameConverter.class)
    private NormalizedDatabaseName database;

    @Option(
            names = "--from-path",
            paramLabel = "<path>",
            description =
                    "'/path/to/directory-containing-dump-or-backup' Path to a directory containing a database dump or backup file to upload.",
            required = true)
    private Path archiveDirectory;

    @Option(
            names = "--to-uri",
            paramLabel = "<uri>",
            arity = "1",
            required = true,
            description = "'neo4j://mydatabaseid.databases.neo4j.io' Bolt URI of the target database.")
    private String boltURI;

    @Option(
            names = "--to-dbid",
            paramLabel = "<databaseid>",
            description =
                    "Database ID of the target database. Required when uploading to a Multi-DB instance, using an instance-based URI (e.g. *.instances.neo4j.io).")
    private String toDbId;

    @Option(
            names = "--to-user",
            defaultValue = "${" + ENV_NEO4J_USERNAME + "}",
            description =
                    "Username of the target database to push this database to. Prompt will ask for a username if not provided. "
                            + "%nDefault:  The value of the " + ENV_NEO4J_USERNAME + " environment variable.")
    private String username;

    @Option(
            names = TO_PASSWORD,
            defaultValue = "${" + ENV_NEO4J_PASSWORD + "}",
            description =
                    "Password of the target database to push this database to. Prompt will ask for a password if not provided. "
                            + "%nDefault:  The value of the " + ENV_NEO4J_PASSWORD + " environment variable.")
    private String password;

    @Option(
            names = "--overwrite-destination",
            arity = "0..1",
            paramLabel = "true|false",
            fallbackValue = "true",
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
            description = "Overwrite the data in the target database.")
    private boolean overwrite;

    @Option(
            names = "--to",
            paramLabel = "<destination>",
            description = "The destination for the upload.",
            defaultValue = "aura",
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String to;

    public UploadCommand(
            ExecutionContext ctx,
            AuraClient.AuraClientBuilder clientBuilder,
            AuraURLFactory auraURLFactory,
            UploadURLFactory uploadURLFactory,
            PushToCloudCLI pushToCloudCLI) {
        super(ctx);
        this.clientBuilder = clientBuilder;
        this.pushToCloudCLI = pushToCloudCLI;
        this.auraURLFactory = auraURLFactory;
        this.uploadURLFactory = uploadURLFactory;
    }

    @Override
    public void execute() {
        try {
            if (!"aura".equals(to)) {
                throw new CommandFailedException(
                        format("'%s' is not a supported destination. Supported destinations are: 'aura'", to));
            }

            if (isBlank(username)) {
                if (isBlank(username = pushToCloudCLI.readLine("%s", "Neo4j aura username (default: neo4j):"))) {
                    username = "neo4j";
                }
            }
            char[] pass;
            if (isBlank(password)) {
                if ((pass = pushToCloudCLI.readPassword("Neo4j aura password for %s:", username)).length == 0) {
                    throw new CommandFailedException(format(
                            "Please supply a password, either by '%s' parameter, '%s' environment variable, or prompt",
                            TO_PASSWORD, ENV_NEO4J_PASSWORD));
                }
            } else {
                pass = password.toCharArray();
            }

            boolean devMode = pushToCloudCLI.readDevMode(DEV_MODE_VAR_NAME);

            AuraConsole auraConsole = auraURLFactory.buildConsoleURI(boltURI, devMode, toDbId);

            AuraClient auraClient = clientBuilder
                    .withAuraConsole(auraConsole)
                    .withUserName(username)
                    .withPassword(pass)
                    .withConsent(overwrite)
                    .withBoltURI(boltURI)
                    .withDefaults()
                    .build();

            Uploader uploader =
                    makeDumpUploader(archiveDirectory, database.name(), ctx, verbose, boltURI, uploadURLFactory);

            uploader.process(auraClient);
        } catch (Exception e) {
            throw new CommandFailedException("Upload command failed", e);
        }
    }
}
