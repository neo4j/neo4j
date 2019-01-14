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

import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link Log} implementation that discards all messages
 */
public final class NullLog implements Log
{
    private static final NullLog INSTANCE = new NullLog();

    private NullLog()
    {
    }

    /**
     * @return A singleton {@link NullLog} instance
     */
    public static NullLog getInstance()
    {
        return INSTANCE;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return false;
    }

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void debug( @Nonnull String message )
    {
    }

    @Override
    public void debug( @Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void debug( @Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void info( @Nonnull String message )
    {
    }

    @Override
    public void info( @Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void info( @Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void warn( @Nonnull String message )
    {
    }

    @Override
    public void warn( @Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void warn( @Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void error( @Nonnull String message )
    {
    }

    @Override
    public void error( @Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void error( @Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        consumer.accept( this );
    }
}
