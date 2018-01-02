/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates a logger for tests, and marks beginning and end of tests with log messages
 */
public class LoggerRule
        extends ExternalResource
{
    private final Level level;
    private Logger logger;
    private String testName;

    public LoggerRule()
    {
        this( Level.INFO );
    }

    public LoggerRule( Level level )
    {
        this.level = level;
    }

    @Override
    protected void before()
            throws Throwable
    {
        logger.info( "Begin test:" + testName );
        super.before();
    }

    @Override
    protected void after()
    {
        super.after();
        logger.info( "Finished test:" + testName );
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        testName = description.getDisplayName();
        logger = Logger.getLogger( description.getTestClass().getName() );
        logger.setLevel( level );
        return super.apply( base, description );
    }

    public Logger getLogger()
    {
        return logger;
    }
}
