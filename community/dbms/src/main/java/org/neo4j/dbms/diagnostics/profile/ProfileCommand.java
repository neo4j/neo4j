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
package org.neo4j.dbms.diagnostics.profile;

import java.nio.file.Path;
import java.time.Duration;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.diagnostics.jmx.JMXDumper;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.time.Clocks;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;
import picocli.CommandLine;

@CommandLine.Command(
        name = "profile",
        header = "Profile a running neo4j process",
        description = "Runs various profilers against a running neo4j VM",
        hidden = true)
public class ProfileCommand extends AbstractAdminCommand {

    @CommandLine.Parameters(description = "Output directory of profiles")
    private Path output;

    @CommandLine.Parameters(
            description = "Duration, how long the profilers should run",
            converter = Converters.DurationConverter.class)
    private Duration duration;

    public ProfileCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected void execute() throws Exception {
        Config config = createPrefilledConfigBuilder().build();
        JMXDumper jmxDumper = new JMXDumper(config, ctx.fs(), ctx.out(), ctx.err(), verbose);
        JmxDump jxmpDump = jmxDumper
                .getJMXDump()
                .orElseThrow(
                        () -> new CommandFailedException("Can not connect to running JVM. Profiling can not be done"));
        // TODO improve output from dumper, its designed only for report command
        SystemNanoClock clock = Clocks.nanoClock();

        try (ProfileTool tool = new ProfileTool()) {
            // TODO add profilers
            tool.start();
            Stopwatch stopwatch = Stopwatch.start();
            while (!stopwatch.hasTimedOut(duration) && tool.hasRunningProfilers()) {
                Thread.sleep(10);
            }
            tool.stop();
            tool.profilers().forEach(this::printFailedProfiler);
        }
    }

    private void addProfiler(ProfileTool tool, Profiler profiler) {
        if (!tool.add(profiler)) {
            ctx.out().println(profiler.getClass().getSimpleName() + " is not available and will not be used");
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
