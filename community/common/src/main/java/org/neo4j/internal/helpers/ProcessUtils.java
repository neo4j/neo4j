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
package org.neo4j.internal.helpers;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.chomp;
import static org.neo4j.string.EncodingUtils.getNativeCharset;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.text.StringTokenizer;
import org.apache.commons.text.matcher.StringMatcherFactory;

public class ProcessUtils {

    public static String executeCommand(String command, Duration timeout) {
        String[] commands = new StringTokenizer(
                        command,
                        StringMatcherFactory.INSTANCE.splitMatcher(),
                        StringMatcherFactory.INSTANCE.quoteMatcher())
                .getTokenArray();
        return executeCommand(commands, timeout);
    }

    public static String executeCommand(String[] commands, Duration timeout) {
        Process process = null;
        try {
            process = new ProcessBuilder(commands).start();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Thread outGobbler = streamGobbler(process.getInputStream(), out);
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            Thread errGobbler = streamGobbler(process.getErrorStream(), err);
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalArgumentException(
                        format("Timed out executing command `%s`", Arrays.toString(commands)));
            }

            outGobbler.join();
            errGobbler.join();
            String output = chomp(out.toString(getNativeCharset()));

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new IllegalArgumentException(format(
                        "Command `%s` failed with exit code %s.%n%s%n%s",
                        Arrays.toString(commands), exitCode, output, chomp(err.toString(getNativeCharset()))));
            }
            return output;
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
}
