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
