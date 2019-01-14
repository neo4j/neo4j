/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    public void debug( @Nonnull String message )
    {
        debugLogger().log( message );
    }

    @Override
    public void debug( @Nonnull String message, @Nonnull Throwable throwable )
    {
        debugLogger().log( message, throwable );
    }

    @Override
    public void debug( @Nonnull String format, @Nullable Object... arguments )
    {
        debugLogger().log( format, arguments );
    }

    @Override
    public void info( @Nonnull String message )
    {
        infoLogger().log( message );
    }

    @Override
    public void info( @Nonnull String message, @Nonnull Throwable throwable )
    {
        infoLogger().log( message, throwable );
    }

    @Override
    public void info( @Nonnull String format, @Nullable Object... arguments )
    {
        infoLogger().log( format, arguments );
    }

    @Override
    public void warn( @Nonnull String message )
    {
        warnLogger().log( message );
    }

    @Override
    public void warn( @Nonnull String message, @Nonnull Throwable throwable )
    {
        warnLogger().log( message, throwable );
    }

    @Override
    public void warn( @Nonnull String format, @Nullable Object... arguments )
    {
        warnLogger().log( format, arguments );
    }

    @Override
    public void error( @Nonnull String message )
    {
        errorLogger().log( message );
    }

    @Override
    public void error( @Nonnull String message, @Nonnull Throwable throwable )
    {
        errorLogger().log( message, throwable );
    }

    @Override
    public void error( @Nonnull String format, @Nullable Object... arguments )
    {
        errorLogger().log( format, arguments );
    }
}
