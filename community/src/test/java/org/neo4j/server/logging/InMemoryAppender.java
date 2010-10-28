/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import java.io.StringWriter;
import java.lang.reflect.Field;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;

public class InMemoryAppender {
    private StringWriter stringWriter = new StringWriter();
    private WriterAppender appender = new WriterAppender(new SimpleLayout(), stringWriter);
    private final org.apache.log4j.Logger log4jLogger;
    private final Level level;
    private Layout layout;

    public InMemoryAppender(Logger logger) {
        this(logger, new SimpleLayout());
    }

    public InMemoryAppender(Logger logger, Layout layout) {
        this(logger, Level.ALL, layout);
    }

    private InMemoryAppender(Logger logger, Level level, Layout layout) {
        this.level = level;
        this.layout = layout;
        log4jLogger = org.apache.log4j.Logger.getLogger(this.getClass());
        changeLogger(logger, log4jLogger);
        reset();

    }
//
//    public InMemoryAppender(Logger logger, Level level) {
//        this(logger, level, new SimpleLayout());
//    }
//
//    public InMemoryAppender(org.apache.log4j.Logger log4jLogger, Level level) {
//        this.log4jLogger = log4jLogger;
//        this.level = level;
//        reset();
//    }

    private void changeLogger(Logger logger, org.apache.log4j.Logger log4jLogger) {
        Field loggerField = findLoggerField(logger);
        try {
            loggerField.setAccessible(true);
            loggerField.set(logger, log4jLogger);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Field findLoggerField(Logger logger) {
        try {
            return logger.getClass().getDeclaredField("logger");
        } catch (Exception e) {
            throw new RuntimeException("The field [logger] must be present for testing", e);
        }
    }

    public void removeAppender() {
        org.apache.log4j.Logger log4jLogger = org.apache.log4j.Logger.getLogger(this.getClass());
        log4jLogger.removeAppender(appender);
    }

    public String toString() {
        return stringWriter.toString();
    }

    public void reset() {
        stringWriter = new StringWriter();
        appender = new WriterAppender(layout, stringWriter);
        log4jLogger.addAppender(appender);
        log4jLogger.setLevel(level);
    }
}
