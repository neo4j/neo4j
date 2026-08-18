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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.jutils.jprocesses.JProcesses;
import org.jutils.jprocesses.model.ProcessInfo;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnection;
import org.neo4j.kernel.diagnostics.DiagnosticsReportInfo;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSources;
import org.neo4j.kernel.diagnostics.DiagnosticsReporter;
import org.neo4j.kernel.diagnostics.DiagnosticsReporterProgress;

/**
 * The core of the {@code neo4j-admin database report} command, extracted so that a diagnostics report can be generated
 * from anywhere that has a {@link Config} and a {@link FileSystemAbstraction} - notably from within a running instance
 * (e.g. fleet management) - rather than only from the command line.
 * <p>
 * Offline providers and tool-provided sources (config files, running processes, JMX) are always available. Authenticated
 * providers additionally require a live {@link DiagnosticsLiveConnection} to the running DBMS; supply one to
 * {@link #generate} (or collect them yourself via {@link #createAndRegisterSources} and
 * {@link DiagnosticsReporter#collectAuthenticatedSources}) to include their classifiers.
 */
public class DiagnosticsReportGenerator {
    private static final DateTimeFormatter filenameDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");

    private final FileSystemAbstraction fs;
    private final Config config;
    private final Path confDir;
    private final PrintStream out;
    private final PrintStream err;
    private final boolean verbose;

    public DiagnosticsReportGenerator(ExecutionContext ctx, Config config, boolean verbose) {
        this.fs = ctx.fs();
        this.config = config;
        this.confDir = ctx.confDir();
        this.out = ctx.out();
        this.err = ctx.err();
        this.verbose = verbose;
    }

    /**
     * Builds a fully wired reporter, validates the requested classifiers, optionally collects authenticated sources and
     * dumps the report. This is the convenience entry point for callers (such as fleet management) that already have a
     * live connection or do not need one.
     *
     * @param connection an open connection to the running DBMS used to collect authenticated sources, or {@code null}
     * to skip authenticated providers. The caller owns the connection and is responsible for closing it.
     */
    public String generate(
            Set<String> classifiers,
            Path reportDir,
            DiagnosticsReporterProgress progress,
            boolean ignoreDiskSpaceCheck,
            Set<String> dbNames,
            DiagnosticsLiveConnection connection)
            throws IOException {
        DiagnosticsReporter reporter = createAndRegisterSources(dbNames);
        validateClassifiers(reporter, classifiers);
        if (connection != null) {
            reporter.collectAuthenticatedSources(classifiers, connection);
        }
        return dump(reporter, classifiers, reportDir, progress, ignoreDiskSpaceCheck);
    }

    public DiagnosticsReporter createAndRegisterSources(Set<String> databaseNames) {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        reporter.registerAllOfflineProviders(config, fs, databaseNames);
        reporter.registerAllAuthenticatedProviders(config, fs, databaseNames);

        // Register sources provided by this tool
        if (fs.isDirectory(confDir)) {
            try {
                Path[] configs = fs.listFiles(confDir, path -> {
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

    public void validateClassifiers(DiagnosticsReporter reporter, Set<String> classifiers) {
        Set<String> availableClassifiers = reporter.getAvailableClassifiers();
        if (classifiers.contains("all")) {
            if (classifiers.size() != 1) {
                classifiers.remove("all");
                throw new CommandFailedException("If you specify 'all' this has to be the only classifier. Found ['"
                        + join("','", classifiers) + "'] as well.");
            }
        } else {
            if (classifiers.equals(Set.of(DiagnosticsReportCommand.DEFAULT_CLASSIFIERS))) {
                classifiers = new HashSet<>(classifiers);
                classifiers.retainAll(availableClassifiers);
            }
            validateOrphanClassifiers(availableClassifiers, classifiers);
        }
    }

    public String dump(
            DiagnosticsReporter reporter,
            Set<String> classifiers,
            Path reportDir,
            DiagnosticsReporterProgress progress,
            boolean ignoreDiskSpaceCheck)
            throws IOException {
        if (reportDir == null) {
            reportDir = Path.of(System.getProperty("java.io.tmpdir"))
                    .resolve("reports")
                    .toAbsolutePath();
        }
        // Resolve host and time once so the archive file name and the manifest inside it always correspond.
        DiagnosticsReportInfo info = DiagnosticsReportInfo.local();
        Path reportFile = reportDir.resolve(getFilename(info));
        out.println("Writing report to " + reportFile.toAbsolutePath());
        reporter.dump(classifiers, reportFile, progress, ignoreDiskSpaceCheck, info);
        out.println("Report generated at " + reportFile.toAbsolutePath());
        return reportFile.toAbsolutePath().toString();
    }

    private void registerJMXSources(DiagnosticsReporter reporter) {
        JMXDumper jmxDumper = new JMXDumper(config, fs, out, err, verbose);
        Optional<JmxDump> jmxDump = jmxDumper.getJMXDump();
        jmxDump.ifPresent(jmx -> {
            reporter.registerSource("threads", jmx.threadDumpSource());
            reporter.registerSource("heap", jmx.heapDump());
            reporter.registerSource("sysprop", jmx.systemProperties());
        });
    }

    private static void validateOrphanClassifiers(Set<String> availableClassifiers, Set<String> orphans) {
        for (String classifier : orphans) {
            if (!availableClassifiers.contains(classifier)) {
                throw new CommandFailedException("Unknown classifier: " + classifier);
            }
        }
    }

    public static String getFilename(DiagnosticsReportInfo info) {
        String safeFilename = info.hostName().replaceAll("[^a-zA-Z0-9._]+", "_");
        return safeFilename + "-" + info.timestamp().toLocalDateTime().format(filenameDateTimeFormatter) + ".zip";
    }

    public static String describeClassifier(String classifier) {
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
