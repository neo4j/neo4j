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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.helpers.NOPLoggerFactory;
import org.slf4j.helpers.NOPMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

/**
 * Capture SLF4J logging and redirect it to an {@link InternalLogProvider}.
 * <p>
 * A call to {@link #setInstantiationContext(Log4jLogProvider,List)} most be done <strong>before</strong>
 * any interactions with libraries that uses SLF4J. This should ideally be done as soon as possible
 * during bootstrapping.
 */
public class SLF4JLogBridge implements SLF4JServiceProvider {
    private static final String REQUESTED_API_VERSION = "2.0.99";
    private static final AtomicReference<InstantiationContext> CONTEXT = new AtomicReference<>();
    private static final Logger LOGGER = StatusLogger.getLogger();

    private ILoggerFactory loggerFactory;
    private IMarkerFactory markerFactory;
    private MDCAdapter mdcAdapter;

    public SLF4JLogBridge() {
        // Service-loaded by SLF4J
    }

    public static void setInstantiationContext(Log4jLogProvider newLogProvider, List<String> classPrefixes) {
        CONTEXT.set(new InstantiationContext(newLogProvider, classPrefixes));
    }

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return markerFactory;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return mdcAdapter;
    }

    @Override
    public String getRequestedApiVersion() {
        return REQUESTED_API_VERSION;
    }

    @Override
    public void initialize() {
        InstantiationContext ctx = CONTEXT.get();
        if (ctx != null) {

            LOGGER.debug(
                    "Initializing [{}] with neo4j log provider with prefix filter {}.",
                    SLF4JLogBridge.class.getSimpleName(),
                    ctx.classPrefixes);
            SLF4JToLog4jMarkerFactory slf4JToLog4jMarkerFactory = new SLF4JToLog4jMarkerFactory();
            markerFactory = slf4JToLog4jMarkerFactory;
            loggerFactory =
                    new SLF4JToLog4jLoggerFactory(ctx.logProvider, slf4JToLog4jMarkerFactory, ctx.classPrefixes);
            mdcAdapter = new SLF4JToLog4jMDCAdapter();
        } else {
            LOGGER.debug("Initializing [{}] with NOP provider.", SLF4JLogBridge.class.getSimpleName());
            markerFactory = new BasicMarkerFactory();
            loggerFactory = new NOPLoggerFactory();
            mdcAdapter = new NOPMDCAdapter();
        }
    }

    private record InstantiationContext(Log4jLogProvider logProvider, List<String> classPrefixes) {}
}
