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

import static org.neo4j.server.NeoBootstrapper.SIGINT;
import static org.neo4j.server.NeoBootstrapper.SIGTERM;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.BootloaderSettings;
import sun.misc.Signal;

class ProcessManager {
    private final Bootloader bootloader;

    ProcessManager(Bootloader bootloader) {
        this.bootloader = bootloader;
    }

    long run(List<String> command, ProcessStages processStages) throws CommandFailedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processStages.preStart(this, processBuilder);
        Process process = null;
        try {
            if (bootloader.verbose) {
                bootloader.environment.out().println("Executing command line: " + String.join(" ", command));
            }
            process = processBuilder.start();

            processStages.postStart(this, process);
            return process.pid();
        } catch (CommandFailedException e) {
            throw e; // rethrow
        } catch (Exception e) {
            if (process != null && process.isAlive()) {
                process.destroy();
            }
            throw new CommandFailedException(
                    "Unexpected error while starting. Aborting. " + e.getClass().getSimpleName() + " : "
                            + e.getMessage(),
                    e);
        }
    }

    void addHomeAndConf(ProcessBuilder processBuilder) {
        Map<String, String> env = processBuilder.environment();
        env.putIfAbsent(Bootloader.ENV_NEO4J_HOME, bootloader.home().toString());
        env.putIfAbsent(Bootloader.ENV_NEO4J_CONF, bootloader.confDir().toString());
    }

    void waitUntilSuccessful(Process process, ExitCodeMessageMapper exitCodeMessageMapper) throws InterruptedException {
        if (process.waitFor() != 0) {
            int code = process.exitValue();
            throw new BootProcessFailureException(exitCodeMessageMapper.map(code), code);
        }
    }

    void installShutdownHook(Process finalProcess) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> killProcess(finalProcess)));
        Runnable onSignal = () -> System.exit(killProcess(finalProcess));
        installSignal(SIGINT, onSignal);
        installSignal(SIGTERM, onSignal);
    }

    private static void installSignal(String signal, Runnable onExit) {
        try {
            Signal.handle(new Signal(signal), s -> onExit.run());
        } catch (Throwable ignored) { // If we for some reason can't install this signal we may just return a different
            // exit code than the actual neo4j-process. No big deal
        }
    }

    private synchronized int killProcess(Process finalProcess) {
        if (finalProcess.isAlive()) {
            finalProcess.destroy();
            while (finalProcess.isAlive()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        deletePid();
        return finalProcess.exitValue();
    }

    Long getPidFromFile() {
        Path pidFile = pidFile();
        try {
            return PidFileHelper.readPid(pidFile);
        } catch (AccessDeniedException e) {
            throw new CommandFailedException("Access denied reading pid file " + pidFile, 1);
        } catch (IOException e) {
            throw new CommandFailedException("Unexpected error reading pid file " + pidFile, e, 1);
        }
    }

    Optional<ProcessHandle> getProcessHandle(long pid) throws CommandFailedException {
        Optional<ProcessHandle> handleOption = ProcessHandle.of(pid);
        if (handleOption.isEmpty() || !handleOption.get().isAlive()) {
            deletePid();
            return Optional.empty();
        }
        return handleOption;
    }

    private void deletePid() {
        PidFileHelper.remove(pidFile());
    }

    void storePid(long pid, boolean throwOnFailure) throws IOException {
        Path pidFilePath = pidFile();
        try {
            PidFileHelper.storePid(pidFile(), pid);
        } catch (AccessDeniedException exception) {
            if (throwOnFailure) {
                throw exception;
            }
            bootloader
                    .environment
                    .err()
                    .printf("Failed to write PID file: Access denied at %s%n", pidFilePath.toAbsolutePath());
        }
    }

    private Path pidFile() {
        return bootloader.config().get(BootloaderSettings.pid_file);
    }
}
