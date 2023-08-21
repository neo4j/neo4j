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
package org.neo4j.logging.log4j;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.LoggerPrintStreamAdaptor;
import org.neo4j.util.VisibleForTesting;

public final class SystemLogger {
    private static final StatusLogger STATUS_LOGGER = StatusLogger.getLogger();

    private static final StatusErrorListener ERROR_LISTENER = new StatusErrorListener();

    private SystemLogger() {}

    /**
     * Install an error listener to capture errors that happens before logging is configured.
     * This should be called as early as possible.
     */
    public static void installErrorListener() {
        ERROR_LISTENER.clear();
        STATUS_LOGGER.registerListener(ERROR_LISTENER);
    }

    /**
     * Check if any errors was encountered during logging setup.
     * This will also stop listening for errors since we should now have a working log provider and will redirect output there.
     * @see SystemLogger#installStdRedirects(InternalLogProvider)
     * @return {@code true} if no errors was encountered.
     */
    public static boolean errorsEncounteredDuringSetup() {
        STATUS_LOGGER.removeListener(ERROR_LISTENER);
        return ERROR_LISTENER.haveErrors();
    }

    @VisibleForTesting
    static void printErrorMessages(PrintStream errorOutput) {
        if (ERROR_LISTENER.haveErrors()) {
            ERROR_LISTENER.getErrors().forEach(errorOutput::println);
        }
    }

    public static void installStdRedirects(InternalLogProvider LogProvider) {
        System.setOut(new LoggerPrintStreamAdaptor(LogProvider.getLog("stdout"), org.neo4j.logging.Level.INFO));
        System.setErr(new LoggerPrintStreamAdaptor(LogProvider.getLog("stderr"), org.neo4j.logging.Level.WARN));
    }

    private static class StatusErrorListener implements StatusListener {
        private final ConcurrentLinkedQueue<String> errorMessages = new ConcurrentLinkedQueue<>();

        @Override
        public void log(StatusData data) {
            errorMessages.add(data.getFormattedStatus());
        }

        @Override
        public Level getStatusLevel() {
            return Level.ERROR;
        }

        @Override
        public void close() throws IOException {}

        void clear() {
            errorMessages.clear();
        }

        boolean haveErrors() {
            return !errorMessages.isEmpty();
        }

        List<String> getErrors() {
            return errorMessages.stream().toList(); // return a copy of the errors
        }
    }
}
