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
package org.neo4j.server.startup;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.PrintStream;
import java.util.Optional;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.io.IOUtils;

abstract class AbstractUnixBootloaderOs extends BootloaderOsAbstraction {
    AbstractUnixBootloaderOs(Bootloader bootloader) {
        super(bootloader);
    }

    @Override
    long start() {
        return bootloader
                .processManager()
                .run(buildStandardStartArguments(), new StartProcess(bootloader.environment.err()));
    }

    private static class StartProcess implements ProcessStages {
        private final PrintStream errorStream;

        private StartProcess(PrintStream errorStream) {
            this.errorStream = errorStream;
        }

        @Override
        public void preStart(ProcessManager processManager, ProcessBuilder processBuilder) {
            // Just inherit stdout, stderr will be "inherited" by the stream gobbler
            processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        }

        @Override
        public void postStart(ProcessManager processManager, Process process) throws Exception {
            ErrorGobbler errorGobbler = new ErrorGobbler(errorStream, process.getErrorStream());
            errorGobbler.start();

            processManager.storePid(process.pid(), true);

            boolean success = errorGobbler.waitUntilFullyFledged();

            // Detach
            errorGobbler.join(MINUTES.toMillis(1));
            IOUtils.closeAll(process.getOutputStream(), process.getErrorStream(), process.getInputStream());

            if (!success) {
                if (process.waitFor(1, MINUTES)) {
                    int code = process.exitValue();
                    throw new BootProcessFailureException(NEO4J_PROCESS_EXITCODE_MAPPER.map(code), code);
                }
                throw new CommandFailedException("Failed to start server.");
            }
        }
    }

    @Override
    void stop(long pid) throws CommandFailedException {
        getProcessIfAlive(pid).ifPresent(ProcessHandle::destroy);
    }

    @Override
    long console() throws CommandFailedException {
        return bootloader.processManager().run(buildStandardStartArguments(), new UnixConsoleProcess());
    }

    static class UnixConsoleProcess extends ConsoleProcess {
        UnixConsoleProcess() {
            super(true);
        }

        @Override
        public void postStart(ProcessManager processManager, Process process) throws Exception {
            processManager.storePid(process.pid(), false);
            super.postStart(processManager, process);
        }
    }

    @Override
    void installService() throws CommandFailedException {
        throw new UnsupportedOperationException("Not supported on this OS");
    }

    @Override
    void uninstallService() throws CommandFailedException {
        throw new UnsupportedOperationException("Not supported on this OS");
    }

    @Override
    void updateService() throws CommandFailedException {
        throw new UnsupportedOperationException("Not supported on this OS");
    }

    @Override
    boolean serviceInstalled() {
        throw new UnsupportedOperationException("Not supported on this OS");
    }

    @Override
    Optional<Long> getPidIfRunning() {
        return getProcessIfAlive(bootloader.processManager().getPidFromFile()).map(ProcessHandle::pid);
    }

    @Override
    boolean isRunning(long pid) {
        return getProcessIfAlive(pid).isPresent();
    }

    private Optional<ProcessHandle> getProcessIfAlive(Long pid) {
        if (pid != null) {
            return bootloader.processManager().getProcessHandle(pid);
        }
        return Optional.empty();
    }
}
