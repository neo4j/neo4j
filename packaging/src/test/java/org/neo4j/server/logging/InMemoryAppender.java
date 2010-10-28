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
    
    public InMemoryAppender(Logger logger, Level level) {
        this(logger, level, new SimpleLayout());
    }
    
    public InMemoryAppender(org.apache.log4j.Logger log4jLogger, Level level) {
        this.log4jLogger = log4jLogger;
        this.level = level;
        reset();
    }
    
    private void changeLogger(Logger logger,
            org.apache.log4j.Logger log4jLogger) {
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
