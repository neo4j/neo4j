/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.dbms;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.diagnostics.profile.ProfileCommand;
import org.neo4j.kernel.diagnostics.DiagnosticsConnectionException;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnection;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnectionFactory;
import org.neo4j.kernel.diagnostics.DiagnosticsReporter;
import org.neo4j.kernel.diagnostics.DiagnosticsReporterProgress;
import org.neo4j.kernel.diagnostics.InteractiveProgress;
import org.neo4j.kernel.diagnostics.NonInteractiveProgress;
import org.neo4j.service.Services;

@Command(
        name = "report",
        header = "Produces a zip/tar of the most common information needed for remote assessments.",
        description =
                "Will collect information about the system and package everything in an archive. If you specify 'all', "
                        + "everything will be included. You can also fine tune the selection by passing classifiers to the tool, "
                        + "e.g 'logs tx threads'.",
        subcommands = {ProfileCommand.class})
public class DiagnosticsReportCommand extends AbstractAdminCommand {
    static final String[] DEFAULT_CLASSIFIERS = {
        "logs", "config", "plugins", "tree", "metrics", "threads", "sysprop", "ps", "version"
    };

    @Option(
            names = "--database",
            paramLabel = "<database>",
            defaultValue = "*",
            description = "Name of the database to report for. Can contain * and ? for globbing. "
                    + "Note that * and ? have special meaning in some shells "
                    + "and might need to be escaped or used with quotes.",
            converter = Converters.DatabaseNamePatternConverter.class)
    private DatabaseNamePattern database;

    @Option(names = "--list", description = "List all available classifiers.")
    private boolean list;

    @Option(
            names = "--ignore-disk-space-check",
            defaultValue = "false",
            arity = "0..1",
            paramLabel = "true|false",
            fallbackValue = "true",
            showDefaultValue = ALWAYS,
            description = "Ignore disk full warning.")
    private boolean ignoreDiskSpaceCheck;

    @Option(
            names = "--to-path",
            paramLabel = "<path>",
            description = "Destination directory for reports. Defaults to a system tmp directory.")
    private Path reportDir;

    @Option(
            names = {"-u", "--username"},
            paramLabel = "<username>",
            defaultValue = "${env:NEO4J_USERNAME}",
            description =
                    "Username for connecting to the running DBMS. Required when a classifier that needs a "
                            + "connection to a live database is selected. Can be specified as the NEO4J_USERNAME environment variable.")
    private String username;

    @Option(
            names = {"-p", "--password"},
            paramLabel = "<password>",
            defaultValue = "${env:NEO4J_PASSWORD}",
            description =
                    "Password for connecting to the running DBMS. Required when a classifier that needs a "
                            + "connection to a live database is selected. Can be specified as the NEO4J_PASSWORD environment variable.")
    private String password;

    @Option(
            names = {"-a", "--address", "--uri"},
            paramLabel = "<address>",
            description = "Address of the DBMS to connect to, including the scheme (e.g. bolt://localhost:7687 or "
                    + "bolt+ssc://localhost:7687). Defaults to an address derived from the instance configuration.")
    private String address;

    @Parameters(arity = "0..*", paramLabel = "<classifier>")
    private Set<String> classifiers = new TreeSet<>(List.of(DEFAULT_CLASSIFIERS));

    // Null means resolve the factory via service loading, so the core module needs no connection-technology dependency.
    private DiagnosticsLiveConnectionFactory connectionFactory;

    public DiagnosticsReportCommand(ExecutionContext ctx) {
        super(ctx);
    }

    void setConnectionFactory(DiagnosticsLiveConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void execute() {
        Config config = getConfig();
        DiagnosticsReportGenerator generator = new DiagnosticsReportGenerator(ctx, config, verbose);

        Set<String> dbNames = getDbNames(config, ctx.fs(), database);
        DiagnosticsReporter reporter = generator.createAndRegisterSources(dbNames);

        if (list) {
            listClassifiers(reporter);
            return;
        }

        generator.validateClassifiers(reporter, classifiers);

        collectAuthenticatedSourcesIfNeeded(config, reporter);

        DiagnosticsReporterProgress progress = buildProgress();

        try {
            generator.dump(reporter, classifiers, reportDir, progress, ignoreDiskSpaceCheck);
        } catch (IOException e) {
            throw new CommandFailedException("Creating archive failed", e);
        }
    }

    private DiagnosticsReporterProgress buildProgress() {
        return System.console() == null
                ? new NonInteractiveProgress(ctx.out(), verbose)
                : new InteractiveProgress(ctx.out(), verbose);
    }

    private void collectAuthenticatedSourcesIfNeeded(Config config, DiagnosticsReporter reporter) {
        Set<String> authenticatedClassifiers = reporter.getAuthenticatedClassifiers();
        if (authenticatedClassifiers.isEmpty()) {
            return;
        }

        Set<String> explicitlyRequested = new TreeSet<>(authenticatedClassifiers);
        explicitlyRequested.retainAll(classifiers);
        boolean viaAll = classifiers.contains("all");
        if (explicitlyRequested.isEmpty() && !viaAll) {
            return;
        }

        if (viaAll && username == null) {
            // 'all' was requested without credentials - skip the authenticated reports rather than failing.
            ctx.out()
                    .println("No credentials provided (--username/--password). Authenticated reports will be omitted.");
            return;
        }

        promptForCredentialsIfNeeded();

        if (username == null || password == null) {
            ctx.out()
                    .printf(
                            "No credentials provided (--username/--password) for classifiers `%s`. Authenticated reports will be omitted.",
                            String.join(",", classifiers));
            return;
        }

        DiagnosticsLiveConnectionFactory factory =
                connectionFactory != null ? connectionFactory : loadConnectionFactory();
        DiagnosticsLiveConnection connection;
        try {
            connection = factory.connect(config, address, username, password);
        } catch (DiagnosticsConnectionException e) {
            // be lenient and continue reporting even if we can't connect to the DBMS
            ctx.out()
                    .println("Failed to connect to the running DBMS: " + e.getMessage()
                            + ": Authenticated reports will be omitted.");
            return;
        }
        try (connection) {
            reporter.collectAuthenticatedSources(classifiers, connection);
        }
    }

    private static DiagnosticsLiveConnectionFactory loadConnectionFactory() {
        return Services.loadAll(DiagnosticsLiveConnectionFactory.class).stream()
                .findFirst()
                .orElseThrow(() -> new CommandFailedException("Unable to connect to the running DBMS: "
                        + "no connection provider is available on the classpath."));
    }

    private void listClassifiers(DiagnosticsReporter reporter) {
        ctx.out().println("All available classifiers:");
        for (String classifier : reporter.getAvailableClassifiers()) {
            ctx.out().printf("  %-12s %s%n", classifier, describeClassifier(reporter, classifier));
        }
    }

    private Config getConfig() {
        return createPrefilledConfigBuilder().build();
    }

    private void promptForCredentialsIfNeeded() {
        if (System.console() == null) {
            return;
        }

        if (username == null) {
            username = System.console().readLine("username: ");
        }

        if (username != null && password == null) {
            password = new String(System.console().readPassword("password: "));
        }
    }

    private static String describeClassifier(DiagnosticsReporter reporter, String classifier) {
        String authenticatedDescription = reporter.describeAuthenticatedClassifier(classifier);
        return authenticatedDescription != null
                ? authenticatedDescription
                : DiagnosticsReportGenerator.describeClassifier(classifier);
    }
}
