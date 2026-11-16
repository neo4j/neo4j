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
package org.neo4j.shell.system;

import static org.neo4j.shell.system.Utils.isWindows;

import org.jline.nativ.CLibrary;

/**
 * Checks whether the standard streams are attached to a terminal.
 * <p>
 * Uses jline's own native library rather than jansi's. jline already ships it as part of its terminal support, and
 * it exposes the same {@code isatty} semantics. In case TTY checking is not possible (no libc, for example on
 * Alpine Linux) the checks fall back to the built in Java {@link System#console()}, which checks whether EITHER
 * STDIN or STDOUT has been redirected.
 */
public final class Tty {
    private static final int STDIN_FILENO = 0;
    private static final int STDOUT_FILENO = 1;
    private static final int STDERR_FILENO = 2;

    private Tty() {}

    /**
     * @return true if STDIN is attached to a terminal, false otherwise (e.g. we are reading from a file).
     */
    public static boolean isStdinTty() {
        return isTty(STDIN_FILENO);
    }

    /**
     * @return true if STDOUT is attached to a terminal, false otherwise (e.g. we are writing to a file).
     */
    public static boolean isStdoutTty() {
        return isTty(STDOUT_FILENO);
    }

    /**
     * @return true if both STDOUT and STDERR are attached to a terminal, false otherwise.
     */
    public static boolean isStdoutAndStderrTty() {
        return isTty(STDOUT_FILENO) && isTty(STDERR_FILENO);
    }

    private static boolean isTty(int fileDescriptor) {
        if (isWindows()) {
            // Input will never be a TTY on windows and isatty seems to be able to block forever on Windows so avoid
            // calling it.
            return System.console() != null;
        }
        try {
            return 1 == CLibrary.isatty(fileDescriptor);
        } catch (Throwable ignored) {
            return System.console() != null;
        }
    }
}
