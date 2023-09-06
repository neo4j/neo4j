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

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.configuration.BootloaderSettings.initial_heap_size;
import static org.neo4j.configuration.BootloaderSettings.max_heap_size;
import static org.neo4j.kernel.info.JvmChecker.NEO4J_JAVA_WARNING_MESSAGE;
import static org.neo4j.kernel.info.JvmChecker.SUPPORTED_JAVA_NAME_PATTERN;
import static org.neo4j.server.startup.Bootloader.ENV_HEAP_SIZE;
import static org.neo4j.server.startup.Bootloader.ENV_JAVA_OPTS;
import static org.neo4j.server.startup.Bootloader.PROP_JAVA_CP;
import static org.neo4j.server.startup.Bootloader.PROP_VM_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.server.NeoBootstrapper;

abstract class BootloaderOsAbstraction {
    static final long UNKNOWN_PID = Long.MAX_VALUE;

    protected final Bootloader bootloader;

    protected BootloaderOsAbstraction(Bootloader bootloader) {
        this.bootloader = bootloader;
    }

    abstract Optional<Long> getPidIfRunning();

    abstract boolean isRunning(long pid);

    abstract long start() throws CommandFailedException;

    abstract void stop(long pid) throws CommandFailedException;

    long console() throws CommandFailedException {
        return bootloader.processManager().run(buildStandardStartArguments(), new ConsoleProcess(true));
    }

    protected static final ExitCodeMessageMapper NEO4J_PROCESS_EXITCODE_MAPPER = exitCode -> switch (exitCode) {
                case NeoBootstrapper.WEB_SERVER_STARTUP_ERROR_CODE -> "Neo4j web server failed to start.";
                case NeoBootstrapper.GRAPH_DATABASE_STARTUP_ERROR_CODE -> "Neo4j server failed to start.";
                case NeoBootstrapper.INVALID_CONFIGURATION_ERROR_CODE -> "Configuration is invalid.";
                case NeoBootstrapper.LICENSE_NOT_ACCEPTED_ERROR_CODE -> "License agreement has not been accepted.";
                default -> "Unexpected Neo4j server failure.";
            } + " See log for more info.";

    static class ConsoleProcess implements ProcessStages {
        private final boolean installShutdownHooksForParentProcess;

        ConsoleProcess(boolean installShutdownHooksForParentProcess) {
            this.installShutdownHooksForParentProcess = installShutdownHooksForParentProcess;
        }

        @Override
        public void preStart(ProcessManager processManager, ProcessBuilder processBuilder) {
            processBuilder.inheritIO();
        }

        @Override
        public void postStart(ProcessManager processManager, Process process) throws Exception {
            if (installShutdownHooksForParentProcess) {
                processManager.installShutdownHook(process);
            }
            processManager.waitUntilSuccessful(process, NEO4J_PROCESS_EXITCODE_MAPPER);
        }
    }

    long admin() throws CommandFailedException {
        MutableList<String> arguments = buildBaseArguments();
        if (bootloader.getEnv(ENV_HEAP_SIZE).isBlank()) {
            // Server config is used as one source of heap settings for admin commands.
            // That leads to ridiculous memory demands especially by simple admin commands
            // like getting store info.
            arguments = arguments.reject(argument -> argument.startsWith("-Xms"));
        }
        return bootloader.processManager().run(arguments.withAll(bootloader.additionalArgs), new AdminProcess());
    }

    private static class AdminProcess implements ProcessStages {
        @Override
        public void preStart(ProcessManager processManager, ProcessBuilder processBuilder) {
            processManager.addHomeAndConf(processBuilder);
            processBuilder.inheritIO();
        }

        @Override
        public void postStart(ProcessManager processManager, Process process) throws Exception {
            processManager.waitUntilSuccessful(
                    process, e -> "Admin command failed to start. See output for more info.");
        }
    }

    abstract void installService() throws CommandFailedException;

    abstract void uninstallService() throws CommandFailedException;

    abstract void updateService() throws CommandFailedException;

    abstract boolean serviceInstalled();

    protected List<String> buildStandardStartArguments() {
        return buildBaseArguments()
                .with("--home-dir=" + bootloader.home())
                .with("--config-dir=" + bootloader.confDir())
                .withAll(bootloader.additionalArgs);
    }

    private MutableList<String> buildBaseArguments() {
        return Lists.mutable
                .with(getJavaCmd())
                .with("-cp")
                .with(getClassPath())
                .withAll(getJvmOpts())
                .with(bootloader.entrypoint.getName());
    }

    static BootloaderOsAbstraction getOsAbstraction(Bootloader context) {
        return SystemUtils.IS_OS_WINDOWS
                ? new WindowsBootloaderOs(context)
                : SystemUtils.IS_OS_MAC_OSX ? new MacBootloaderOs(context) : new UnixBootloaderOs(context);
    }

    protected String getJavaCmd() {
        Path java = getJava();
        checkJavaVersion();
        return java.toString();
    }

    private void printBadRuntime() {
        var err = bootloader.environment.err();
        err.println("WARNING! You are using an unsupported Java runtime.");
        err.println("* " + NEO4J_JAVA_WARNING_MESSAGE);
        err.println("* Please see https://neo4j.com/docs/ for Neo4j installation instructions.");
    }

    private static Path getJava() {
        Optional<String> currentCommand = ProcessHandle.current().info().command();
        return Path.of(
                currentCommand.orElseThrow(() -> new IllegalStateException("Wasn't able to figure out java binary")));
    }

    private void checkJavaVersion() {
        int version = bootloader.environment.version().feature();
        if (version != 17 && version != 21) {
            // too new java
            printBadRuntime();
        } else {
            // correct version
            String runtime = bootloader.getProp(PROP_VM_NAME);
            if (!SUPPORTED_JAVA_NAME_PATTERN.matcher(runtime).matches()) {
                printBadRuntime();
            }
        }
    }

    private static String bytesToSuitableJvmString(long bytes) {
        // the JVM accepts k,m,g but we go with k to avoid conversion loss
        return Math.max(bytes / ByteUnit.kibiBytes(1), 1) + "k";
    }

    protected List<String> getJvmOpts() {
        // If JAVA_OPTS is provided, it has the highest priority
        // and we just use that as it is without any modification
        // or added logic
        String envJavaOptions = bootloader.getEnv(ENV_JAVA_OPTS);
        if (isNotEmpty(envJavaOptions)) {
            if (isNotEmpty(bootloader.getEnv(ENV_HEAP_SIZE))) {
                bootloader.environment.err().println("WARNING! HEAP_SIZE is ignored, because JAVA_OPTS is set");
            }

            // We need to turn a list of JVM options provided as one string into a list of individual options.
            // We don't have a code that does exactly that, but SettingValueParsers.JVM_ADDITIONAL turns
            // options provided as one string into a 'list' of individual options separated by a new line.
            return List.of(
                    SettingValueParsers.JVM_ADDITIONAL.parse(envJavaOptions).split(System.lineSeparator()));
        }

        return buildJvmOpts();
    }

    private List<String> buildJvmOpts() {
        MutableList<String> opts = Lists.mutable.empty();

        var config = bootloader.config();
        String jvmAdditionals = config.get(BootloaderSettings.additional_jvm);
        if (isNotEmpty(jvmAdditionals)) {
            opts.withAll(List.of(jvmAdditionals.split(System.lineSeparator())));
        }

        if (config.get(BootloaderSettings.gc_logging_enabled)) {
            opts.with(String.format(
                    "%s:file=%s::filecount=%s,filesize=%s",
                    config.get(BootloaderSettings.gc_logging_options),
                    config.get(GraphDatabaseSettings.logs_directory).resolve("gc.log"),
                    config.get(BootloaderSettings.gc_logging_rotation_keep_number),
                    bytesToSuitableJvmString(config.get(BootloaderSettings.gc_logging_rotation_size))));
        }
        opts.with("-Dfile.encoding=UTF-8");
        selectHeapSettings(opts);
        return opts;
    }

    private void selectHeapSettings(MutableList<String> opts) {
        String envHeapSize = bootloader.getEnv(ENV_HEAP_SIZE);
        if (isNotEmpty(envHeapSize)) {
            // HEAP_SIZE env. variable has highest priority
            opts.with("-Xms" + envHeapSize).with("-Xmx" + envHeapSize);
            return;
        }

        var config = bootloader.config();

        Long xmsConfigValue = config.get(initial_heap_size);
        var xmsValue = xmsConfigValue != null ? bytesToSuitableJvmString(xmsConfigValue) : null;
        if (xmsValue != null) {
            opts.with("-Xms" + xmsValue);
        }

        Long xmxConfigValue = config.get(max_heap_size);
        var xmxValue = xmxConfigValue != null ? bytesToSuitableJvmString(xmxConfigValue) : null;
        if (xmxValue != null) {
            opts.with("-Xmx" + xmxValue);
        }
    }

    protected String getClassPath() {
        String libCp = classPathFromDir(bootloader.config().get(BootloaderSettings.lib_directory));

        List<String> paths = Lists.mutable.with(
                classPathFromDir(bootloader.config().get(GraphDatabaseSettings.plugin_dir)),
                classPathFromDir(bootloader.confDir()),
                StringUtils.isNotBlank(libCp) ? libCp : bootloader.getProp(PROP_JAVA_CP));
        return paths.stream().filter(StringUtils::isNotBlank).collect(Collectors.joining(File.pathSeparator));
    }

    private static String classPathFromDir(Path dir) {
        try {
            if (Files.isDirectory(dir) && !FileUtils.isDirectoryEmpty(dir)) {
                return dir.toAbsolutePath() + File.separator + "*";
            }
        } catch (IOException e) { // Ignore. Default to this JVMs classpath
        }
        return null;
    }
}
