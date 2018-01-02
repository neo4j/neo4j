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
package org.neo4j.logging;

/**
 * An abstract implementation of {@link Log}, providing implementations
 * for the shortcut methods (debug, info, warn, error) that delegate
 * to the appropriate {@link Logger} (as obtained by {@link Log#debugLogger()},
 * {@link Log#infoLogger()}, {@link Log#warnLogger()} and
 * {@link Log#errorLogger()} respectively).
 */
public abstract class AbstractLog implements Log
{
    @Override
    public void debug( String message )
    {
        debugLogger().log( message );
    }

    @Override
    public void debug( String message, Throwable throwable )
    {
        debugLogger().log( message, throwable );
    }

    @Override
    public void debug( String format, Object... arguments )
    {
        debugLogger().log( format, arguments );
    }

    @Override
    public void info( String message )
    {
        infoLogger().log( message );
    }

    @Override
    public void info( String message, Throwable throwable )
    {
        infoLogger().log( message, throwable );
    }

    @Override
    public void info( String format, Object... arguments )
    {
        infoLogger().log( format, arguments );
    }

    @Override
    public void warn( String message )
    {
        warnLogger().log( message );
    }

    @Override
    public void warn( String message, Throwable throwable )
    {
        warnLogger().log( message, throwable );
    }

    @Override
    public void warn( String format, Object... arguments )
    {
        warnLogger().log( format, arguments );
    }

    @Override
    public void error( String message )
    {
        errorLogger().log( message );
    }

    @Override
    public void error( String message, Throwable throwable )
    {
        errorLogger().log( message, throwable );
    }

    @Override
    public void error( String format, Object... arguments )
    {
        errorLogger().log( format, arguments );
    }
}
