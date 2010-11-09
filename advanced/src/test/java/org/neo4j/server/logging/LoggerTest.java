package org.neo4j.server.logging;

import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;


public class LoggerTest
{

    @Test
    public void testParsingLog() {
        Properties props = new Properties();
        props.put( "log4j.rootLogger", "DEBUG, R" );
        props.put( "log4j.appender.R", "org.apache.log4j.RollingFileAppender" );
        props.put( "log4j.appender.R.File","target/neo4j.log" );
        props.put( "log4j.appender.R.layout","org.apache.log4j.PatternLayout" );
        props.put( "log4j.appender.R.layout.ConversionPattern","%p %t %c - %m%n" );
        PropertyConfigurator.configure(props);
        Logger log = Logger.getLogger( getClass());
        org.apache.log4j.Logger log4jLogger = org.apache.log4j.Logger.getLogger( getClass() );
        String message = String.format("No configuration file at [%s]", "%N");
        log4jLogger.error( String.format( "%%N") );
        log.error( message );
    }
}
