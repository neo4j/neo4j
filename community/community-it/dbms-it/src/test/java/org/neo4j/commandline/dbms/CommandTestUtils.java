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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.fs.FileSystemAbstraction;

public class CommandTestUtils {
    /**
     * Creates an {@link ExecutionContext} which captures System.out and System.err and will include
     * captured prints in an exception if the {@code command} fails. This allows commands to run w/o the
     * need to print to the system-wide System.out/System.err and still get all that information on failure.
     *
     * @param homeDir home directory to give to the {@link ExecutionContext}.
     * @param confDir config directory to give to the {@link ExecutionContext}.
     * @param fs file system this is run on.
     * @param command to set up and run the command, given this created {@link ExecutionContext}.
     */
    public static void withSuppressedOutput(
            Path homeDir,
            Path confDir,
            FileSystemAbstraction fs,
            ThrowingConsumer<CapturingExecutionContext, Throwable> command) {
        var rawOut = new ByteArrayOutputStream();
        var rawErr = new ByteArrayOutputStream();
        var out = new PrintStream(rawOut);
        var err = new PrintStream(rawErr);
        var executionContext = new CapturingExecutionContext(homeDir, confDir, rawOut, rawErr, out, err, fs);
        try (out;
                err) {
            command.accept(executionContext);
        } catch (Throwable e) {
            throw new RuntimeException(
                    format("%nCaptured System.out:%n%s%nCaptured System.err:%n%s", rawOut, rawErr), e);
        }
    }

    public static class CapturingExecutionContext extends ExecutionContext {
        private final ByteArrayOutputStream rawOut;
        private final ByteArrayOutputStream rawErr;

        CapturingExecutionContext(
                Path homePath,
                Path confPath,
                ByteArrayOutputStream rawOut,
                ByteArrayOutputStream rawErr,
                PrintStream out,
                PrintStream err,
                FileSystemAbstraction fs) {
            super(homePath, confPath, out, err, fs);
            this.rawOut = rawOut;
            this.rawErr = rawErr;
        }

        public String outAsString() {
            out().flush();
            return rawOut.toString();
        }

        public String errAsString() {
            err().flush();
            return rawErr.toString();
        }
    }
}
