/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.util;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.Visitor;

import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.LogMarker;
import org.neo4j.kernel.logging.Logging;

/* Utility to test log messages in integration tests. */
public class TestLogging implements Logging
{
    private Map<Class, TestLogger> messageLoggers = new HashMap<>();
    private Map<Class, TestLogger> consoleLoggers = new HashMap<>();

    @Override
    public TestLogger getMessagesLog( Class loggingClass )
    {
        if(!messageLoggers.containsKey( loggingClass ))
        {
            messageLoggers.put( loggingClass, new TestLogger() );
        }
        return messageLoggers.get( loggingClass );
    }

    @Override
    public ConsoleLogger getConsoleLog( Class loggingClass )
    {
        if(!consoleLoggers.containsKey( loggingClass ))
        {
            consoleLoggers.put( loggingClass, new TestLogger(){
                @Override
                public void logMessage( String msg, Throwable cause, boolean flush, LogMarker marker )
                {
                    // Console log still uses the logMessage methods, so we work around that here for now.
                    // We should write a separate TestLogger for the console logging when time permits.
                    super.info( msg, cause );
                }
            });
        }
        return new ConsoleLogger( consoleLoggers.get( loggingClass ) );
    }

    /**
     * Use this to do asserts on console logging
     *
     * @param loggingClass
     * @return
     */
    public TestLogger getDelegatedConsoleLog( Class loggingClass )
    {
        return consoleLoggers.get( loggingClass );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    private void visitLog( TestLogger logger, Visitor<TestLogger.LogCall,RuntimeException> logVisitor )
    {
        if(logger != null)
        {
            logger.visitLogCalls( logVisitor );
        }
    }

    public void visitMessagesLog( Class loggingClass, Visitor<TestLogger.LogCall,RuntimeException> logVisitor )
    {
        visitLog( messageLoggers.get( loggingClass ), logVisitor );
    }

    public void visitConsoleLog( Class loggingClass, Visitor<TestLogger.LogCall,RuntimeException> logVisitor )
    {
        visitLog( consoleLoggers.get( loggingClass ), logVisitor );
    }
}
