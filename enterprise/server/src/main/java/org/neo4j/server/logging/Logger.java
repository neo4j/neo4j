/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.logging;

import java.util.HashSet;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;


public class Logger
{

    public static Logger log = Logger.getLogger(Logger.class);

    private static HashSet<String> illegalParameters = new HashSet<String>();

    static {
        illegalParameters.add("%b");
        illegalParameters.add("%h");
        illegalParameters.add("%s");
        illegalParameters.add("%c");
        illegalParameters.add("%d");
        illegalParameters.add("%o");
        illegalParameters.add("%x");
        illegalParameters.add("%e");
        illegalParameters.add("%f");
        illegalParameters.add("%g");
        illegalParameters.add("%a");
        illegalParameters.add("%t");
        illegalParameters.add("%%");
        illegalParameters.add("%n");
    }

    org.apache.log4j.Logger logger;

    public static Logger getLogger(Class<?> clazz) {
        return new Logger(clazz);
    }
    
    public static Logger getLogger(String logger) {
        return new Logger(logger);
    }

    public Logger(Class<?> clazz) {
        logger = org.apache.log4j.Logger.getLogger(clazz);
    }

    public Logger(String str) {
        logger = org.apache.log4j.Logger.getLogger(str);
    }

    public void log(Priority priority, String message, Throwable throwable) {
        logger.log(priority, message, throwable);
    }

    public void log(Level level, String message, Object... parameters) {

        for (Object obj : parameters) {
            if (obj != null) {
                String s = obj.toString();
                if (illegalParameters.contains(s.toLowerCase())) {
                    log.warn("Failed to log, parameters like " + s + " are not supported.");
                    return;
                }
            }
        }

        if (logger.isEnabledFor(level)) {
            logger.log(level, String.format(message, parameters));
        }
    }

    public void fatal(String message, Object... parameters) {
        log(Level.FATAL, message, parameters);
    }

    public void error(String message, Object... parameters) {
        log(Level.ERROR, message, parameters);
    }

    public void error(Throwable e) {
        log(Level.ERROR, "", e);
    }

    public void warn(Throwable e) {
        log(Level.WARN, "", e);
    }

    public void warn(String message, Object... parameters) {
        log(Level.WARN, message, parameters);
    }

    public void info(String message, Object... parameters) {
        log(Level.INFO, message, parameters);
    }

    public void debug(String message, Object... parameters) {
        log(Level.DEBUG, message, parameters);
    }

    public void trace(String message, Object... parameters) {
        log(Level.TRACE, message, parameters);
    }
}
