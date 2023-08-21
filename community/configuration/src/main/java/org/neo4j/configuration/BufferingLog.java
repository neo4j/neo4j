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
package org.neo4j.configuration;

import java.util.ArrayDeque;
import java.util.Queue;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jLogMessage;
import org.neo4j.logging.Neo4jMessageSupplier;

/**
 * Buffers all messages sent to it, and is able to replay those messages into
 * another Logger.
 * <p>
 * This can be used to start up services that need logging when they start, but
 * where, for one reason or another, we have not yet set up proper logging in
 * the application lifecycle.
 * <p>
 * This will replay messages in the order they are received, *however*, it will
 * not preserve the time stamps of the original messages.
 * <p>
 * You should not use this for logging messages where the time stamps are
 * important.
 * <p>
 * You should also not use this logger, when there is a risk that it can be
 * subjected to an unbounded quantity of log messages, since the buffer keeps
 * all messages until it gets a chance to replay them.
 */
public class BufferingLog implements InternalLog {
    @FunctionalInterface
    private interface LogMessage {
        void replayInto(InternalLog other);
    }

    private final Queue<LogMessage> buffer = new ArrayDeque<>();

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public synchronized void debug(String message) {
        buffer.add(other -> other.debug(message));
    }

    @Override
    public synchronized void debug(String message, Throwable throwable) {
        buffer.add(other -> other.debug(message, throwable));
    }

    @Override
    public synchronized void debug(String format, Object... arguments) {
        buffer.add(other -> other.debug(format, arguments));
    }

    @Override
    public synchronized void info(String message) {
        buffer.add(other -> other.info(message));
    }

    @Override
    public synchronized void info(String message, Throwable throwable) {
        buffer.add(other -> other.info(message, throwable));
    }

    @Override
    public synchronized void info(String format, Object... arguments) {
        buffer.add(other -> other.info(format, arguments));
    }

    @Override
    public synchronized void warn(String message) {
        buffer.add(other -> other.warn(message));
    }

    @Override
    public synchronized void warn(String message, Throwable throwable) {
        buffer.add(other -> other.warn(message, throwable));
    }

    @Override
    public synchronized void warn(String format, Object... arguments) {
        buffer.add(other -> other.warn(format, arguments));
    }

    @Override
    public synchronized void error(String message) {
        buffer.add(other -> other.error(message));
    }

    @Override
    public synchronized void error(String message, Throwable throwable) {
        buffer.add(other -> other.error(message, throwable));
    }

    @Override
    public synchronized void error(String format, Object... arguments) {
        buffer.add(other -> other.error(format, arguments));
    }

    @Override
    public synchronized void debug(Neo4jLogMessage message) {
        buffer.add(other -> other.debug(message));
    }

    @Override
    public synchronized void debug(Neo4jMessageSupplier supplier) {
        buffer.add(other -> other.debug(supplier));
    }

    @Override
    public synchronized void info(Neo4jLogMessage message) {
        buffer.add(other -> other.info(message));
    }

    @Override
    public synchronized void info(Neo4jMessageSupplier supplier) {
        buffer.add(other -> other.info(supplier));
    }

    @Override
    public synchronized void warn(Neo4jLogMessage message) {
        buffer.add(other -> other.warn(message));
    }

    @Override
    public synchronized void warn(Neo4jMessageSupplier supplier) {
        buffer.add(other -> other.warn(supplier));
    }

    @Override
    public synchronized void error(Neo4jLogMessage message) {
        buffer.add(other -> other.error(message));
    }

    @Override
    public synchronized void error(Neo4jMessageSupplier supplier) {
        buffer.add(other -> other.error(supplier));
    }

    @Override
    public synchronized void error(Neo4jLogMessage message, Throwable throwable) {
        buffer.add(other -> other.error(message, throwable));
    }

    /**
     * Replays buffered messages and clears the buffer.
     *
     * @param other the log to reply into
     */
    public void replayInto(InternalLog other) {
        synchronized (buffer) {
            LogMessage message = buffer.poll();
            while (message != null) {
                message.replayInto(other);
                message = buffer.poll();
            }
        }
    }
}
