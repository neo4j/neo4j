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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.internal.helpers.ProcessUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.time.SystemNanoClock;

class NmtProfiler extends PeriodicProfiler {

    private final JmxDump dump;
    private final FileSystemAbstraction fs;
    private final Path dir;
    private final String jcmd;
    private volatile boolean detailed;

    NmtProfiler(JmxDump dump, FileSystemAbstraction fs, Path dir, Duration interval, SystemNanoClock clock) {
        super(interval, clock);
        this.dump = dump;
        this.fs = fs;
        this.dir = dir;
        try {
            fs.mkdirs(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        jcmd = ProcessUtils.getJavaExecutable()
                .getParent()
                .resolve("jcmd")
                .toAbsolutePath()
                .toString();
    }

    @Override
    protected void start() {
        checkDetailed();
        setBaseline();
        writeReport("full", getFullReport());
        super.start();
    }

    @Override
    protected void stop() {
        writeReport("full", getFullReport());
        super.stop();
    }

    @Override
    protected void tick() {
        writeReport("diff", getDiffReport());
    }

    @Override
    protected boolean available() {
        String vmArgs = dump.vmArguments();
        if (vmArgs.contains("-XX:NativeMemoryTracking") && !vmArgs.contains("-XX:NativeMemoryTracking=off")) {
            try {
                // Check jcmd available
                executeCommand(jcmd);
                return true;
            } catch (RuntimeException ignored) {
                setFailure(new RuntimeException("'jcmd' tool not found. Make sure it is installed"));
            }
        } else {
            setFailure(new RuntimeException("Java NMT not enabled on the server. Temporarily add "
                    + BootloaderSettings.additional_jvm.name()
                    + "=-XX:NativeMemoryTracking=summary to neo4j.conf and restart the server to enable."
                    + " Please note that this has a performance impact."));
        }
        return false;
    }

    private void checkDetailed() {
        String vmArgs = dump.vmArguments();
        detailed = vmArgs.contains("-XX:NativeMemoryTracking=detail");
    }

    private void writeReport(String type, String report) {
        String name = String.format(
                "nmt-%s-%s-%s.txt",
                type, detailed ? "detail" : "summary", clock.instant().toString());
        try (OutputStream os = fs.openAsOutputStream(dir.resolve(name), false)) {
            os.write(report.getBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void setBaseline() {
        executeNmt("baseline");
    }

    private String getFullReport() {
        return executeNmt(detailed ? "detail" : "summary");
    }

    private String getDiffReport() {
        return executeNmt(detailed ? "detail.diff" : "summary.diff");
    }

    private String executeNmt(String arg) {
        return executeCommand(jcmd, Long.toString(dump.getPid()), "VM.native_memory", arg);
    }

    static String executeCommand(String... commands) {
        return ProcessUtils.executeCommandWithOutput(commands, Duration.ofMinutes(1));
    }
}
