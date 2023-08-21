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

import static org.neo4j.server.startup.Environment.FULLY_FLEDGED;
import static org.neo4j.string.EncodingUtils.getNativeCharset;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;

/**
 * A stream gobbler that will propagate an error stream from the child process to the parent, emulating {@link ProcessBuilder.Redirect#INHERIT}.
 * We use this to inspect the stream for specific signaling characters.
 */
class ErrorGobbler extends Thread {
    private final PrintStream parentProcessStdErr;
    private final BufferedReader childProcessStdErr;
    private final CountDownLatch blockParent = new CountDownLatch(1);

    // Happens-before guaranteed by latch above
    private volatile IOException exception;
    private volatile boolean success;

    ErrorGobbler(PrintStream parentProcessStdErr, InputStream childProcessStdErr) {
        super(ErrorGobbler.class.getSimpleName());
        this.parentProcessStdErr = parentProcessStdErr;
        this.childProcessStdErr = new BufferedReader(new InputStreamReader(childProcessStdErr, getNativeCharset()));
        setDaemon(true);
        setUncaughtExceptionHandler((t, e) -> e.printStackTrace(parentProcessStdErr));
    }

    /**
     * Will block until the child process signal that it's ready or the process ends.
     * @return {@code ture} if the signal was observed, {@code false} otherwise.
     */
    boolean waitUntilFullyFledged() throws IOException {
        try {
            blockParent.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for child process to bootstrap.", e);
        }
        // Propagate any exception from gobbler thread
        if (exception != null) {
            throw new IOException("Error while waiting for child process.", exception);
        }
        return success;
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = childProcessStdErr.readLine()) != null) {
                if (line.length() > 0 && line.charAt(0) == FULLY_FLEDGED) {
                    success = true;
                    blockParent.countDown();
                } else {
                    // Redirect output from child to parent error
                    parentProcessStdErr.println(line);
                }
            }
        } catch (IOException e) {
            exception = e;
        } finally {
            parentProcessStdErr.flush();
            blockParent.countDown();
        }
    }
}
