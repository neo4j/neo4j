package org.neo4j.server.logging;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;

public class Logger {
    org.apache.log4j.Logger logger;
    
    public static Logger getLogger(Class<?> clazz) {
        return new Logger(clazz);
    }

    public Logger(Class<?> clazz) {
        logger = org.apache.log4j.Logger.getLogger(clazz);
    }
    
    public void log(Priority priority, String message, Throwable throwable) {
        logger.log(priority, message, throwable);
    }

    public void log(Level level, String message, Object ... parameters) {
        if (logger.isEnabledFor(level)) {
            logger.log(level, String.format(message, parameters));          
        }
    }

    public void fatal(String message, Object ... parameters) {
        log(Level.FATAL, message, parameters);
    }

    public void error(String message, Object ... parameters) {
        log(Level.ERROR, message, parameters);
    }
    
    public void error(Throwable e) {
        log(Level.ERROR, "", e);
    }
    
    public void warn(Throwable e) {
        log(Level.WARN, "", e);
    }

    public void warn(String message, Object ... parameters) {
        log(Level.WARN, message, parameters);
    }       
    
    public void info(String message, Object ... parameters) {
        log(Level.INFO, message, parameters);
    }

    public void debug(String message, Object ... parameters) {
        log(Level.DEBUG, message, parameters);
    }
    
    public void trace(String message, Object ... parameters) {
        log(Level.TRACE, message, parameters);
    }   
}
