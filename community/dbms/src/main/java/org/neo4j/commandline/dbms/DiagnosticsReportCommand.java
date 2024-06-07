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

import static java.lang.String.join;
import static org.apache.commons.text.StringEscapeUtils.escapeCsv;
import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsString;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.jutils.jprocesses.JProcesses;
import org.jutils.jprocesses.model.ProcessInfo;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.dbms.diagnostics.profile.ProfileCommand;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSources;
import org.neo4j.kernel.diagnostics.DiagnosticsReporter;
import org.neo4j.kernel.diagnostics.DiagnosticsReporterProgress;
import org.neo4j.kernel.diagnostics.InteractiveProgress;
import org.neo4j.kernel.diagnostics.NonInteractiveProgress;

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
    private static final DateTimeFormatter filenameDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");

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

    @Parameters(arity = "0..*", paramLabel = "<classifier>")
    private Set<String> classifiers = new TreeSet<>(List.of(DEFAULT_CLASSIFIERS));

    private JMXDumper jmxDumper;

    public DiagnosticsReportCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    public void execute() {
        Config config = getConfig();

        jmxDumper = new JMXDumper(config, ctx.fs(), ctx.out(), ctx.err(), verbose);

        Set<String> dbNames = getDbNames(config, ctx.fs(), database);
        DiagnosticsReporter reporter = createAndRegisterSources(config, dbNames);

        if (list) {
            listClassifiers(reporter.getAvailableClassifiers());
            return;
        }

        validateClassifiers(reporter);

        DiagnosticsReporterProgress progress = buildProgress();

        // Start dumping
        try {
            if (reportDir == null) {
                reportDir = Path.of(System.getProperty("java.io.tmpdir"))
                        .resolve("reports")
                        .toAbsolutePath();
            }
            Path reportFile = reportDir.resolve(getDefaultFilename());
            ctx.out().println("Writing report to " + reportFile.toAbsolutePath());
            reporter.dump(classifiers, reportFile, progress, ignoreDiskSpaceCheck);
        } catch (IOException e) {
            throw new CommandFailedException("Creating archive failed", e);
        }
    }

    private static String getDefaultFilename() throws UnknownHostException {
        String hostName = InetAddress.getLocalHost().getHostName();
        String safeFilename = hostName.replaceAll("[^a-zA-Z0-9._]+", "_");
        return safeFilename + "-" + LocalDateTime.now().format(filenameDateTimeFormatter) + ".zip";
    }

    private DiagnosticsReporterProgress buildProgress() {
        return System.console() == null
                ? new NonInteractiveProgress(ctx.out(), verbose)
                : new InteractiveProgress(ctx.out(), verbose);
    }

    private void validateClassifiers(DiagnosticsReporter reporter) {
        Set<String> availableClassifiers = reporter.getAvailableClassifiers();
        if (classifiers.contains("all")) {
            if (classifiers.size() != 1) {
                classifiers.remove("all");
                throw new CommandFailedException("If you specify 'all' this has to be the only classifier. Found ['"
                        + join("','", classifiers) + "'] as well.");
            }
        } else {
            if (classifiers.equals(Set.of(DEFAULT_CLASSIFIERS))) {
                classifiers = new HashSet<>(classifiers);
                classifiers.retainAll(availableClassifiers);
            }
            validateOrphanClassifiers(availableClassifiers, classifiers);
        }
    }

    private static void validateOrphanClassifiers(Set<String> availableClassifiers, Set<String> orphans) {
        for (String classifier : orphans) {
            if (!availableClassifiers.contains(classifier)) {
                throw new CommandFailedException("Unknown classifier: " + classifier);
            }
        }
    }

    private void listClassifiers(Set<String> availableClassifiers) {
        ctx.out().println("All available classifiers:");
        for (String classifier : availableClassifiers) {
            ctx.out().printf("  %-10s %s%n", classifier, describeClassifier(classifier));
        }
    }

    private DiagnosticsReporter createAndRegisterSources(Config config, Set<String> databaseNames) {
        DiagnosticsReporter reporter = new DiagnosticsReporter();

        FileSystemAbstraction fs = ctx.fs();
        reporter.registerAllOfflineProviders(config, fs, databaseNames);

        // Register sources provided by this tool
        if (fs.isDirectory(ctx.confDir())) {
            try {
                Path[] configs = fs.listFiles(ctx.confDir(), path -> {
                    String fileName = path.getFileName().toString();
                    return fileName.startsWith("neo4j") && fileName.endsWith(".conf");
                });

                for (Path cfg : configs) {
                    String destination = "config/" + cfg.getFileName();
                    if (fs.isDirectory(cfg)) {
                        // Likely a kubernetes config
                        DiagnosticsReportSources.newDiagnosticsMatchingFiles(
                                        destination + "/",
                                        fs,
                                        cfg,
                                        path -> !fs.isDirectory(path)
                                                && !path.getFileName()
                                                        .toString()
                                                        .startsWith("."))
                                .forEach(conf -> reporter.registerSource("config", conf));
                    } else {
                        // Normal config file
                        reporter.registerSource(
                                "config", DiagnosticsReportSources.newDiagnosticsFile(destination, fs, cfg));
                    }
                }
            } catch (IOException e) {
                reporter.registerSource(
                        "config",
                        newDiagnosticsString(
                                "config error", () -> "Error reading files in directory: " + e.getMessage()));
                throw new RuntimeException(e);
            }
        }

        Path serverLogsConfig = config.get(GraphDatabaseSettings.server_logging_config_path);
        if (fs.fileExists(serverLogsConfig)) {
            reporter.registerSource(
                    "config",
                    DiagnosticsReportSources.newDiagnosticsFile("config/server-logs.xml", fs, serverLogsConfig));
        }

        Path userLogsConfig = config.get(GraphDatabaseSettings.user_logging_config_path);
        if (fs.fileExists(userLogsConfig)) {
            reporter.registerSource(
                    "config", DiagnosticsReportSources.newDiagnosticsFile("config/user-logs.xml", fs, userLogsConfig));
        }

        reporter.registerSource("ps", runningProcesses());

        // Online connection
        registerJMXSources(reporter);
        return reporter;
    }

    private void registerJMXSources(DiagnosticsReporter reporter) {
        Optional<JmxDump> jmxDump;
        jmxDump = jmxDumper.getJMXDump();
        jmxDump.ifPresent(jmx -> {
            reporter.registerSource("threads", jmx.threadDumpSource());
            reporter.registerSource("heap", jmx.heapDump());
            reporter.registerSource("sysprop", jmx.systemProperties());
        });
    }

    private Config getConfig() {
        return createPrefilledConfigBuilder().build();
    }

    static String describeClassifier(String classifier) {
        return switch (classifier) {
            case "logs" -> "include log files";
            case "config" -> "include configuration files";
            case "plugins" -> "include a view of the plugin directory";
            case "tree" -> "include a view of the tree structure of the data directory";
            case "tx" -> "include transaction logs";
            case "metrics" -> "include metrics";
            case "threads" -> "include a thread dump of the running instance";
            case "heap" -> "include a heap dump";
            case "sysprop" -> "include a list of java system properties";
            case "raft" -> "include the raft log";
            case "ccstate" -> "include the current cluster state";
            case "ps" -> "include a list of running processes";
            case "version" -> "include version of neo4j";
            default -> throw new IllegalArgumentException("Unknown classifier: " + classifier);
        };
    }

    private static DiagnosticsReportSource runningProcesses() {
        return newDiagnosticsString("ps.csv", () -> {
            List<ProcessInfo> processesList = JProcesses.getProcessList();

            StringBuilder sb = new StringBuilder();
            sb.append(escapeCsv("Process PID"))
                    .append(',')
                    .append(escapeCsv("Process Name"))
                    .append(',')
                    .append(escapeCsv("Process Time"))
                    .append(',')
                    .append(escapeCsv("User"))
                    .append(',')
                    .append(escapeCsv("Virtual Memory"))
                    .append(',')
                    .append(escapeCsv("Physical Memory"))
                    .append(',')
                    .append(escapeCsv("CPU usage"))
                    .append(',')
                    .append(escapeCsv("Start Time"))
                    .append(',')
                    .append(escapeCsv("Priority"))
                    .append(',')
                    .append(escapeCsv("Full command"))
                    .append('\n');

            for (final ProcessInfo processInfo : processesList) {
                sb.append(processInfo.getPid())
                        .append(',')
                        .append(escapeCsv(processInfo.getName()))
                        .append(',')
                        .append(processInfo.getTime())
                        .append(',')
                        .append(escapeCsv(processInfo.getUser()))
                        .append(',')
                        .append(processInfo.getVirtualMemory())
                        .append(',')
                        .append(processInfo.getPhysicalMemory())
                        .append(',')
                        .append(processInfo.getCpuUsage())
                        .append(',')
                        .append(processInfo.getStartTime())
                        .append(',')
                        .append(processInfo.getPriority())
                        .append(',')
                        .append(escapeCsv(processInfo.getCommand()))
                        .append('\n');
            }
            return sb.toString();
        });
    }
}
