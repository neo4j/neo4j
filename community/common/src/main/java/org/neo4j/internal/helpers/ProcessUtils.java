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
package org.neo4j.internal.helpers;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.chomp;
import static org.neo4j.string.EncodingUtils.getNativeCharset;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringTokenizer;
import org.apache.commons.text.matcher.StringMatcherFactory;

public final class ProcessUtils {

    private ProcessUtils() {}

    /**
     * Get the path to the {@code java} executable that is running this Java program.
     * <p>
     * This is useful for starting other Java programs using the same exact version of Java.
     * <p>
     * This value is computed from the {@code java.home} system property.
     *
     * @return The path to the {@code java} executable that launched this Java process.
     */
    public static Path getJavaExecutable() {
        String javaHome = System.getProperty("java.home");
        return Paths.get(javaHome, "bin", "java");
    }

    /**
     * Get the current classpath as a list of file names.
     * @return The list of file names that makes the classpath.
     */
    public static List<String> getClassPathList() {
        return Arrays.asList(getClassPath().split(File.pathSeparator));
    }

    public static void setClassPath(String classPath) {
        System.setProperty("java.class.path", classPath);
    }

    public static void amendClassPath(String entry) {
        System.setProperty("java.class.path", getClassPath() + File.pathSeparator + entry);
    }

    /**
     * Get the classpath as a single string of all the classpath file entries, separated by the path separator.
     * <p>
     * This is based on the {@code java.class.path} system property.
     * @see File#pathSeparator
     * @return The current classpath.
     */
    public static String getClassPath() {
        return System.getProperty("java.class.path");
    }

    /**
     * Get additional export/open module options that is required to be able to start neo4j as a java process
     * and defined by neo4j own {@code jdk.custom.options} system property.
     *
     * @return array of options that can be passed to the java launcher.
     */
    public static List<String> getModuleOptions() {
        var moduleOptions = System.getProperty("jdk.custom.options");
        if (StringUtils.isEmpty(moduleOptions)) {
            return emptyList();
        }
        return Arrays.stream(moduleOptions.split(" "))
                .filter(StringUtils::isNotBlank)
                .map(String::trim)
                .toList();
    }

    /**
     * Start java process with java that is defined in {@code java.home}, with classpath that is defined by {@code java.class.path} and
     * with additional module options defined by {@code jdk.custom.options} system property and with provided additional arguments.
     * By default, new process started with inherited io option.
     *
     * @param arguments additional arguments that should be passed to new process.
     * @return newly started java process.
     */
    public static Process start(String... arguments) throws IOException {
        return start(ProcessBuilder::inheritIO, arguments);
    }

    /**
     * Start java process with java that is defined in {@code java.home}, with classpath that is defined by {@code java.class.path} and
     * with additional module options defined by {@code jdk.custom.options} system property and with provided additional arguments.
     *
     * @param configurator process builder additional configurator.
     * @param arguments additional arguments that should be passed to new process.
     * @return newly started java process.
     */
    public static Process start(Consumer<ProcessBuilder> configurator, String... arguments) throws IOException {
        ArrayList<String> args = javaExecutableCommandWith(arguments);
        ProcessBuilder processBuilder = new ProcessBuilder(args);
        configurator.accept(processBuilder);
        return processBuilder.start();
    }

    /**
     * Starts a java process with the provided arguments and wait for it to finish. Classpath and system properties will be inherited
     * from the current java process.
     *
     * @param out stdout buffer.
     * @param err stderr buffer.
     * @param timeout time to wait for the process to finish.
     * @param arguments arguments to the java executable.
     * @return process exit code.
     * @throws IOException on failure to allocate argument file.
     */
    public static int executeJava(
            ByteArrayOutputStream out, ByteArrayOutputStream err, Duration timeout, String... arguments)
            throws IOException {
        ArrayList<String> args = javaExecutableCommandWith(arguments);
        return executeCommand(out, err, args.toArray(String[]::new), timeout);
    }

    /**
     * Execute a command and wait for it to finish. Any output to stdout will be returned.
     *
     * @param command to execute, e.g. {@code ls -al}.
     * @param timeout time to wait for the process to finish.
     * @return the output of the executed command.
     * @throws IllegalArgumentException if the command return a non-zero exit code or if the timeout is reached.
     */
    public static String executeCommandWithOutput(String command, Duration timeout) {
        String[] commands = new StringTokenizer(
                        command,
                        StringMatcherFactory.INSTANCE.splitMatcher(),
                        StringMatcherFactory.INSTANCE.quoteMatcher())
                .getTokenArray();
        return executeCommandWithOutput(commands, timeout);
    }

    /**
     * Execute a command and wait for it to finish. Any output to stdout will be returned.
     *
     * @param commands to execute, e.g. {@code ["ls","-al"]}.
     * @param timeout time to wait for the process to finish.
     * @return the output of the executed command.
     * @throws IllegalArgumentException if the command return a non-zero exit code or if the timeout is reached.
     */
    public static String executeCommandWithOutput(String[] commands, Duration timeout) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exitCode = executeCommand(out, err, commands, timeout);

        String output = chomp(out.toString(getNativeCharset()));
        if (exitCode != 0) {
            throw new IllegalArgumentException(format(
                    "Command `%s` failed with exit code %s.%n%s%n%s",
                    Arrays.toString(commands), exitCode, output, chomp(err.toString(getNativeCharset()))));
        }
        return output;
    }

    private static int executeCommand(
            ByteArrayOutputStream out, ByteArrayOutputStream err, String[] commands, Duration timeout) {
        Process process = executeProcess(out, err, commands, timeout);
        return process.exitValue();
    }

    private static Process executeProcess(
            ByteArrayOutputStream out, ByteArrayOutputStream err, String[] commands, Duration timeout) {
        Process process = null;
        try {
            var builder = new ProcessBuilder(commands);
            process = builder.start();
            Thread outGobbler = streamGobbler(process.getInputStream(), out);
            Thread errGobbler = streamGobbler(process.getErrorStream(), err);
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalArgumentException(
                        format("Timed out executing command `%s`", Arrays.toString(commands)));
            }

            outGobbler.join();
            errGobbler.join();
            return process;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Interrupted while executing command", e);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

    private static Thread streamGobbler(InputStream from, OutputStream to) {
        Thread thread = new Thread(() -> {
            try {
                from.transferTo(to);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        thread.start();
        return thread;
    }

    /**
     * Constructs a command to start a new java process, matching current process classpath and properties.
     *
     * @param arguments to be passed to the java process.
     * @return a list of arguments that can be passed to directly to {@link ProcessBuilder#ProcessBuilder(List)}.
     * @throws IOException on failure to allocate argument file.
     */
    private static ArrayList<String> javaExecutableCommandWith(String[] arguments) throws IOException {
        var args = new ArrayList<String>();
        args.add(getJavaExecutable().toString());
        var moduleOptions = getModuleOptions();
        if (!moduleOptions.isEmpty()) {
            args.addAll(moduleOptions);
        }

        // Classpath can get very long and that can upset Windows, so write it to a file
        Path p = Files.createTempFile("jvm", ".args");
        p.toFile().deleteOnExit();
        Files.writeString(p, systemProperties() + "-cp " + wrapSpaces(getClassPath()), StandardCharsets.UTF_8);

        args.add("@" + p.normalize());
        args.addAll(Arrays.asList(arguments));
        return args;
    }

    private static String systemProperties() {
        StringBuilder builder = new StringBuilder();
        Properties properties = System.getProperties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String name = entry.getKey().toString();
            if (!isJdkProperty(name)) {
                builder.append(systemProperty(name, entry.getValue().toString()));
                builder.append(" ");
            }
        }
        return builder.toString();
    }

    private static boolean isJdkProperty(String name) {
        return name.startsWith("java")
                || name.startsWith("os")
                || name.startsWith("sun")
                || name.startsWith("user")
                || name.startsWith("line");
    }

    private static String systemProperty(String key, String value) {
        return "-D" + key + "=" + value;
    }

    private static String wrapSpaces(String value) {
        return value.replace(" ", "\" \"");
    }
}
