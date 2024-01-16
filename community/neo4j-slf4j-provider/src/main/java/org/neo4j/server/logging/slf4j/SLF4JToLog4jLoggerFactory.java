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
import org.apache.logging.log4j.LogManager;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

public class SLF4JToLog4jLoggerFactory implements ILoggerFactory {
    private final Log4jLogProvider logProvider;
    private final SLF4JToLog4jMarkerFactory markerFactory;
    private final List<String> classPrefixes;

    public SLF4JToLog4jLoggerFactory(
            Log4jLogProvider logProvider, SLF4JToLog4jMarkerFactory markerFactory, List<String> classPrefixes) {
        this.logProvider = logProvider;
        this.markerFactory = markerFactory;
        this.classPrefixes = classPrefixes;
    }

    @Override
    public Logger getLogger(String name) {
        if (!shouldInclude(name)) {
            return NOPLogger.NOP_LOGGER;
        }

        String key = remapRootLogger(name);
        return new SLF4JToLog4jLogger(markerFactory, logProvider.getLog(key), name);
    }

    private boolean shouldInclude(String name) {
        if (classPrefixes.isEmpty()) {
            return true; // No filter
        }

        for (String classPrefix : classPrefixes) {
            if (name.startsWith(classPrefix)) {
                return true;
            }
        }

        return false;
    }

    private static String remapRootLogger(String name) {
        return Logger.ROOT_LOGGER_NAME.equals(name) ? LogManager.ROOT_LOGGER_NAME : name;
    }
}
