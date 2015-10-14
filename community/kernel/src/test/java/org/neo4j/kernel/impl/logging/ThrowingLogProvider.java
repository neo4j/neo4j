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
package org.neo4j.kernel.impl.logging;

import java.util.LinkedList;

import org.neo4j.function.Consumer;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.AbstractLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

public class ThrowingLogProvider extends AbstractLogProvider<Log>
{
    private volatile boolean doThrow;
    private final LinkedList<String> loggedStatements = new LinkedList<>();

    public void makeNextLogStatementThrow()
    {
        this.doThrow = true;
    }

    public boolean hasLogged( String statement )
    {
        synchronized ( loggedStatements )
        {
            for ( String logged : loggedStatements )
            {
                if ( logged.equals( statement ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    protected Log buildLog( Class loggingClass )
    {
        return new ThrowingLog();
    }

    @Override
    protected Log buildLog( String name )
    {
        return new ThrowingLog();
    }

    public class ThrowingLog extends AbstractLog
    {
        @Override
        public boolean isDebugEnabled()
        {
            return true;
        }

        @Override
        public Logger debugLogger()
        {
            return new ThrowingLogger();
        }

        @Override
        public Logger infoLogger()
        {
            return new ThrowingLogger();
        }

        @Override
        public Logger warnLogger()
        {
            return new ThrowingLogger();
        }

        @Override
        public Logger errorLogger()
        {
            return new ThrowingLogger();
        }

        @Override
        public void bulk( Consumer<Log> consumer )
        {
            throw new UnsupportedOperationException();
        }
    }

    public class ThrowingLogger implements Logger
    {
        @Override
        public void log( String message )
        {
            if ( doThrow )
            {
                doThrow = false;
                throw new OutOfMemoryError( "Just kidding, really" );
            }

            synchronized ( loggedStatements )
            {
                loggedStatements.add( message );
            }
        }

        @Override
        public void log( String message, Throwable throwable )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log( String format, Object... arguments )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulk( Consumer<Logger> consumer )
        {
            throw new UnsupportedOperationException();
        }
    }
}
