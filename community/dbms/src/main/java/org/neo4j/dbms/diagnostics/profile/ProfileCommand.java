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
package org.neo4j.dbms.diagnostics.profile;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.dbms.archive.StandardCompressionFormat.GZIP;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.NonInteractiveProgress;
import org.neo4j.time.Clocks;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;
import picocli.CommandLine;

@CommandLine.Command(
        name = "profile",
        header = "Profile a running neo4j process",
        description =
                "Runs various profilers against a running neo4j VM. Note: This is a beta version. Behavior and surface will change in future versions.",
        hidden = true)
public class ProfileCommand extends AbstractAdminCommand {

    @CommandLine.Parameters(description = "Output directory of profiles")
    private Path output;

    @CommandLine.Parameters(
            description = "Duration, how long the profilers should run",
            converter = Converters.DurationConverter.class)
    private Duration duration;

    @CommandLine.Parameters(description = "The selected profilers to run. Valid values: ${COMPLETION-CANDIDATES}")
    private Set<ProfilerSource> profilers = Set.of(ProfilerSource.values());

    @CommandLine.Option(
            names = "--skip-compression",
            description = "Keeps the result in a directory structure instead of compressing")
    private boolean skipCompression;

    public enum ProfilerSource {
        JFR,
        THREADS,
        NMT
    }

    public ProfileCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected void execute() throws Exception {
        if (IS_OS_WINDOWS) {
            ctx.out().println("This command is currently not supported on Windows.");
            return;
        }
        if (duration.isNegative() || duration.isZero()) {
            ctx.out().println("Duration needs to be positive");
            return;
        }
        FileSystemAbstraction fs = ctx.fs();
        if (fs.isDirectory(output) && fs.listFiles(output).length > 0) {
            ctx.out().println("Output directory needs to be empty");
            return;
        }
        if (profilers.isEmpty()) {
            profilers = Set.of(ProfilerSource.values());
        }
        Config config = createPrefilledConfigBuilder().build();
        try (JmxDump jmxDump = getJmxDump(fs, config)) {
            // TODO improve output from dumper, its designed only for report command
            SystemNanoClock clock = Clocks.nanoClock();

            ctx.out()
                    .printf(
                            "Profilers %s selected. Duration %s. Output directory %s%n",
                            profilers, DURATION.valueToString(duration), output.toAbsolutePath());
            try (ProfileTool tool = new ProfileTool()) {
                if (profilers.contains(ProfilerSource.JFR)) {
                    addProfiler(tool, new JfrProfiler(jmxDump, fs, output.resolve("jfr"), duration, clock));
                }
                if (profilers.contains(ProfilerSource.THREADS)) {
                    addProfiler(
                            tool,
                            new JstackProfiler(jmxDump, fs, output.resolve("threads"), Duration.ofSeconds(1), clock));
                }
                if (profilers.contains(ProfilerSource.NMT)) {
                    addProfiler(
                            tool, new NmtProfiler(jmxDump, fs, output.resolve("nmt"), Duration.ofSeconds(10), clock));
                }
                if (!tool.hasProfilers()) {
                    ctx.out().println("No profilers to run");
                    return;
                }
                installShutdownHook(tool);
                tool.start();
                Stopwatch stopwatch = Stopwatch.start();
                NonInteractiveProgress progress = new NonInteractiveProgress(ctx.out(), true);
                while (!stopwatch.hasTimedOut(duration) && tool.hasRunningProfilers()) {
                    Thread.sleep(10);
                    progress.percentChanged(
                            (int) (stopwatch.elapsed(TimeUnit.MILLISECONDS) * 100 / duration.toMillis()));
                }
                tool.stop();
                progress.finished();
                List<Profiler> profilers = Iterators.asList(tool.profilers().iterator());
                if (!stopwatch.hasTimedOut(duration)) {
                    ctx.out().println("Profiler stopped before expected duration.");
                }
                if (!profilers.isEmpty()) {
                    profilers.forEach(this::printFailedProfiler);
                    if (profilers.stream().allMatch(profiler -> profiler.failure() != null)) {
                        ctx.out().println("All profilers failed.");
                    } else {
                        ctx.out().println("Profiler results:");
                        for (Path path : fs.listFiles(output)) {
                            try (var fileStream = fs.streamFilesRecursive(path, false)) {
                                List<Path> files = fileStream
                                        .map(FileHandle::getRelativePath)
                                        .toList();
                                ctx.out()
                                        .printf(
                                                "%s/ [%d %s]%n",
                                                output.relativize(path),
                                                files.size(),
                                                files.size() > 1 ? "files" : "file");
                                int numFilesToPrint = 3;
                                for (int i = 0; i < files.size() && i <= numFilesToPrint; i++) {
                                    if (i < numFilesToPrint) {
                                        ctx.out().printf("\t%s%n", files.get(i));
                                    } else {
                                        ctx.out().printf("\t...%n");
                                    }
                                }
                            }
                        }

                        if (!skipCompression) {
                            Path archive = output.resolve(String.format("profile-%s.gzip", clock.instant()));
                            ctx.out().printf("%nCompressing result into %s", archive.getFileName());
                            Dumper dumper = new Dumper(ctx.out());
                            dumper.dump(
                                    output, output, dumper.openForDump(archive), GZIP, path -> path.equals(archive));
                            for (Path path : fs.listFiles(output, fs::isDirectory)) {
                                fs.deleteRecursively(path);
                            }
                        }
                    }
                }
            }
        }
    }

    private JmxDump getJmxDump(FileSystemAbstraction fs, Config config) {
        return new JMXDumper(config, fs, ctx.out(), ctx.err(), verbose)
                .getJMXDump()
                .orElseThrow(() ->
                        new CommandFailedException("Can not connect to running Neo4j. Profiling can not be done"));
    }

    private void installShutdownHook(ProfileTool tool) {
        // Ensure we stop the tool when the JVM dies. (Mostly in case of CTRL-C)
        Runtime.getRuntime().addShutdownHook(new Thread(tool::stop));
    }

    private void addProfiler(ProfileTool tool, Profiler profiler) {
        if (!tool.add(profiler)) {
            ctx.out().println(profiler.getClass().getSimpleName() + " is not available and will not be used");
            RuntimeException failure = profiler.failure();
            if (failure != null) {
                ctx.out().println(failure.getMessage());
            }
        }
    }

    private void printFailedProfiler(Profiler profiler) {
        if (profiler.failure() != null) {
            ctx.out()
                    .println(profiler.getClass().getSimpleName() + " failed with: "
                            + profiler.failure().getMessage());
            if (verbose) {
                profiler.failure().printStackTrace(ctx.err());
            }
        }
    }
}
