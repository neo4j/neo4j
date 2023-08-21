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
package org.neo4j.server.logging.slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.BridgeAware;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogBuilder;
import org.slf4j.Marker;
import org.slf4j.spi.LoggingEventBuilder;

public class SLF4JToLog4jEventBuilder implements LoggingEventBuilder {
    private static final String FQCN = SLF4JToLog4jEventBuilder.class.getName();

    private final SLF4JToLog4jMarkerFactory markerFactory;
    private final LogBuilder logBuilder;
    private final List<Object> arguments = new ArrayList<>();
    private String message = null;
    private Map<String, String> keyValuePairs = null;

    SLF4JToLog4jEventBuilder(SLF4JToLog4jMarkerFactory markerFactory, LogBuilder logBuilder) {
        this.markerFactory = markerFactory;
        this.logBuilder = logBuilder;
        if (logBuilder instanceof BridgeAware bridge) {
            bridge.setEntryPoint(FQCN);
        }
    }

    @Override
    public LoggingEventBuilder setCause(Throwable cause) {
        logBuilder.withThrowable(cause);
        return this;
    }

    @Override
    public LoggingEventBuilder addMarker(Marker marker) {
        logBuilder.withMarker(markerFactory.getLog4jMarker(marker));
        return this;
    }

    @Override
    public LoggingEventBuilder addArgument(Object p) {
        arguments.add(p);
        return this;
    }

    @Override
    public LoggingEventBuilder addArgument(Supplier<?> objectSupplier) {
        arguments.add(objectSupplier.get());
        return this;
    }

    @Override
    public LoggingEventBuilder addKeyValue(String key, Object value) {
        if (keyValuePairs == null) {
            keyValuePairs = new HashMap<>();
        }
        keyValuePairs.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public LoggingEventBuilder addKeyValue(String key, Supplier<Object> valueSupplier) {
        if (keyValuePairs == null) {
            keyValuePairs = new HashMap<>();
        }
        keyValuePairs.put(key, String.valueOf(valueSupplier.get()));
        return this;
    }

    @Override
    public LoggingEventBuilder setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public LoggingEventBuilder setMessage(Supplier<String> messageSupplier) {
        message = messageSupplier.get();
        return this;
    }

    @Override
    public void log() {
        if (keyValuePairs == null || keyValuePairs.isEmpty()) {
            logBuilder.log(message, arguments.toArray());
        } else {
            try (CloseableThreadContext.Instance ignored = CloseableThreadContext.putAll(keyValuePairs)) {
                logBuilder.log(message, arguments.toArray());
            }
        }
    }

    @Override
    public void log(String message) {
        setMessage(message);
        log();
    }

    @Override
    public void log(String message, Object arg) {
        setMessage(message);
        addArgument(arg);
        log();
    }

    @Override
    public void log(String message, Object arg0, Object arg1) {
        setMessage(message);
        addArgument(arg0);
        addArgument(arg1);
        log();
    }

    @Override
    public void log(String message, Object... args) {
        setMessage(message);
        for (Object arg : args) {
            addArgument(arg);
        }
        log();
    }

    @Override
    public void log(Supplier<String> messageSupplier) {
        setMessage(messageSupplier);
        log();
    }
}
