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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.slf4j.Marker;
import org.slf4j.spi.LocationAwareLogger;
import org.slf4j.spi.LoggingEventBuilder;
import org.slf4j.spi.NOPLoggingEventBuilder;

class SLF4JToLog4jLogger implements LocationAwareLogger {
    private static final String FQCN = SLF4JToLog4jLogger.class.getName();

    private final SLF4JToLog4jMarkerFactory markerFactory;
    private final ExtendedLogger logger;
    private final String name;

    SLF4JToLog4jLogger(SLF4JToLog4jMarkerFactory markerFactory, ExtendedLogger logger, String name) {
        this.markerFactory = markerFactory;
        this.logger = logger;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void trace(String msg) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.TRACE, null);
        }
    }

    @Override
    public void trace(String format, Object arg) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.TRACE, null);
        }
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.TRACE, null);
        }
    }

    @Override
    public void trace(String format, Object... arguments) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.TRACE, null);
        }
    }

    @Override
    public void trace(String msg, Throwable t) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, msg, null, t, Level.TRACE, null);
        }
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return isTraceEnabled();
    }

    @Override
    public void trace(Marker marker, String msg) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.TRACE, marker);
        }
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.TRACE, marker);
        }
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.TRACE, marker);
        }
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, format, argArray, null, Level.TRACE, marker);
        }
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        if (isTraceEnabled()) {
            emitLogMessage(FQCN, msg, null, t, Level.TRACE, marker);
        }
    }

    @Override
    public void debug(String msg) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.DEBUG, null);
        }
    }

    @Override
    public void debug(String format, Object arg) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.DEBUG, null);
        }
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.DEBUG, null);
        }
    }

    @Override
    public void debug(String format, Object... arguments) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.DEBUG, null);
        }
    }

    @Override
    public void debug(String msg, Throwable t) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, msg, null, t, Level.DEBUG, null);
        }
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return isDebugEnabled();
    }

    @Override
    public void debug(Marker marker, String msg) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.DEBUG, marker);
        }
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.DEBUG, marker);
        }
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.DEBUG, marker);
        }
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.DEBUG, marker);
        }
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        if (isDebugEnabled()) {
            emitLogMessage(FQCN, msg, null, t, Level.DEBUG, marker);
        }
    }

    @Override
    public void info(String msg) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.INFO, null);
        }
    }

    @Override
    public void info(String format, Object arg) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.INFO, null);
        }
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.INFO, null);
        }
    }

    @Override
    public void info(String format, Object... arguments) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.INFO, null);
        }
    }

    @Override
    public void info(String format, Throwable t) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, format, null, t, Level.INFO, null);
        }
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return isInfoEnabled();
    }

    @Override
    public void info(Marker marker, String msg) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.INFO, marker);
        }
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.INFO, marker);
        }
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.INFO, marker);
        }
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.INFO, marker);
        }
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        if (isInfoEnabled()) {
            emitLogMessage(FQCN, msg, null, t, Level.INFO, marker);
        }
    }

    @Override
    public void warn(String msg) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.WARN, null);
        }
    }

    @Override
    public void warn(String format, Object arg) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.WARN, null);
        }
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.WARN, null);
        }
    }

    @Override
    public void warn(String format, Object... arguments) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.WARN, null);
        }
    }

    @Override
    public void warn(String format, Throwable t) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, format, null, t, Level.WARN, null);
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return isWarnEnabled();
    }

    @Override
    public void warn(Marker marker, String msg) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.WARN, marker);
        }
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.WARN, marker);
        }
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.WARN, marker);
        }
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.WARN, marker);
        }
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        if (isWarnEnabled()) {
            emitLogMessage(FQCN, msg, null, t, Level.WARN, marker);
        }
    }

    @Override
    public void error(String msg) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.ERROR, null);
        }
    }

    @Override
    public void error(String format, Object arg) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.ERROR, null);
        }
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.ERROR, null);
        }
    }

    @Override
    public void error(String format, Object... arguments) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.ERROR, null);
        }
    }

    @Override
    public void error(String format, Throwable t) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, format, null, t, Level.ERROR, null);
        }
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return isErrorEnabled();
    }

    @Override
    public void error(Marker marker, String msg) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, msg, null, null, Level.ERROR, marker);
        }
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg}, null, Level.ERROR, marker);
        }
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, format, new Object[] {arg1, arg2}, null, Level.ERROR, marker);
        }
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, format, arguments, null, Level.ERROR, marker);
        }
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        if (isErrorEnabled()) {
            emitLogMessage(FQCN, msg, null, t, Level.ERROR, marker);
        }
    }

    @Override
    public void log(Marker slf4jMarker, String fqcn, int slf4jLevel, String message, Object[] argArray, Throwable t) {
        Level log4jLevel = getLog4jLevel(slf4jLevel);

        emitLogMessage(fqcn, message, argArray, t, log4jLevel, slf4jMarker);
    }

    private void emitLogMessage(
            String fqcn, String message, Object[] argArray, Throwable t, Level log4jLevel, Marker slf4jMarker) {
        if (argArray == null) {
            logger.logMessage(fqcn, log4jLevel, toLog4jMarker(slf4jMarker), new SimpleMessage(message), t);
        } else {
            // ParameterizedMessage matches SLF4J's {} anchors
            Message msg = new ParameterizedMessage(message, argArray, t);
            Throwable realThrowable = t != null ? t : msg.getThrowable();
            logger.logMessage(fqcn, log4jLevel, toLog4jMarker(slf4jMarker), msg, realThrowable);
        }
    }

    private org.apache.logging.log4j.Marker toLog4jMarker(Marker slf4jMarker) {
        if (slf4jMarker != null) {
            return markerFactory.getLog4jMarker(slf4jMarker);
        }
        return null;
    }

    private static Level getLog4jLevel(int i) {
        return switch (i) {
            case TRACE_INT -> Level.TRACE;
            case DEBUG_INT -> Level.DEBUG;
            case INFO_INT -> Level.INFO;
            case WARN_INT -> Level.WARN;
            default -> Level.ERROR;
        };
    }

    @Override
    public LoggingEventBuilder makeLoggingEventBuilder(org.slf4j.event.Level slf4jLevel) {
        Level log4jLevel = getLog4jLevel(slf4jLevel.toInt());
        return new SLF4JToLog4jEventBuilder(markerFactory, logger.atLevel(log4jLevel));
    }

    @Override
    public LoggingEventBuilder atTrace() {
        if (isTraceEnabled()) {
            return new SLF4JToLog4jEventBuilder(markerFactory, logger.atTrace());
        }
        return NOPLoggingEventBuilder.singleton();
    }

    @Override
    public LoggingEventBuilder atDebug() {
        if (isDebugEnabled()) {
            return new SLF4JToLog4jEventBuilder(markerFactory, logger.atDebug());
        }
        return NOPLoggingEventBuilder.singleton();
    }

    @Override
    public LoggingEventBuilder atInfo() {
        if (isInfoEnabled()) {
            return new SLF4JToLog4jEventBuilder(markerFactory, logger.atInfo());
        }
        return NOPLoggingEventBuilder.singleton();
    }

    @Override
    public LoggingEventBuilder atWarn() {
        if (isWarnEnabled()) {
            return new SLF4JToLog4jEventBuilder(markerFactory, logger.atWarn());
        }
        return NOPLoggingEventBuilder.singleton();
    }

    @Override
    public LoggingEventBuilder atError() {
        if (isErrorEnabled()) {
            return new SLF4JToLog4jEventBuilder(markerFactory, logger.atError());
        }
        return NOPLoggingEventBuilder.singleton();
    }
}
