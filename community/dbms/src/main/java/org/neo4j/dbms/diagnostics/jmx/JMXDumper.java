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
package org.neo4j.dbms.diagnostics.jmx;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Facilitates JMX Dump for current running Neo4j instance.
 */
public class JMXDumper {
    private final Config config;
    private final FileSystemAbstraction fs;
    private final PrintStream err;
    private final boolean verbose;
    private final PrintStream out;

    public JMXDumper(Config config, FileSystemAbstraction fs, PrintStream out, PrintStream err, boolean verbose) {
        this.config = config;
        this.fs = fs;
        this.err = err;
        this.verbose = verbose;
        this.out = out;
    }

    public Optional<JmxDump> getJMXDump() {
        out.println("Finding running instance of neo4j");

        Optional<Long> pid = getPid();
        if (pid.isPresent()) {
            return getJMXDump(pid.get());
        } else {
            out.println("No running instance of neo4j was found. Online reports will be omitted.");
            return Optional.empty();
        }
    }

    private Optional<JmxDump> getJMXDump(long pid) {
        try {
            LocalVirtualMachine vm = LocalVirtualMachine.from(pid);
            out.println("Attached to running process with process id " + pid);
            try {
                JmxDump jmxDump = JmxDump.connectTo(vm.getJmxAddress(), pid);
                jmxDump.attachSystemProperties(vm.getSystemProperties());
                out.println("Connected to JMX endpoint");
                return Optional.of(jmxDump);
            } catch (IOException e) {
                printError("Unable to communicate with JMX endpoint. Reason: " + e.getMessage(), e);
            }
        } catch (java.lang.NoClassDefFoundError | UnsatisfiedLinkError e) {
            printError(
                    "Unable to attach to process. Reason: JDK is not available, please point "
                            + "environment variable JAVA_HOME to a valid JDK location with `attach` library.",
                    e);
        } catch (IOException e) {
            printError("Unable to connect to process with process id " + pid + ". Reason: " + e.getMessage(), e);
        }

        return Optional.empty();
    }

    private void printError(String message, Throwable e) {
        err.println(message);
        if (verbose && e != null) {
            e.printStackTrace(err);
        }
    }

    private void printError(String message) {
        printError(message, null);
    }

    private Optional<Long> getPid() {
        Path pidFile = config.get(BootloaderSettings.pid_file);
        if (this.fs.fileExists(pidFile)) {
            // The file cannot be opened with write permissions on Windows
            try (InputStream inputStream = Files.newInputStream(pidFile, StandardOpenOption.READ);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String pidFileContent = reader.readLine();
                try {
                    return Optional.of(Long.parseLong(pidFileContent));
                } catch (NumberFormatException e) {
                    printError(pidFile + " does not contain a valid id. Found: " + pidFileContent);
                }
            } catch (IOException e) {
                printError("Error reading the .pid file. Reason: " + e.getMessage(), e);
            }
        }
        return Optional.empty();
    }
}
