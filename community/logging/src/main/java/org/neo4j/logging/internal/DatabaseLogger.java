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
package org.neo4j.logging.internal;

import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.logging.Logger;

public class DatabaseLogger implements Logger
{
    private final DatabaseLogContext logContext;
    private final Supplier<Logger> delegate;

    DatabaseLogger( DatabaseLogContext logContext, Supplier<Logger> delegate )
    {
        this.logContext = logContext;
        this.delegate = delegate;
    }

    @Override
    public void log( @Nonnull String message )
    {
        delegate.get().log( withPrefix( message ) );
    }

    @Override
    public void log( @Nonnull String message, @Nonnull Throwable throwable )
    {
        delegate.get().log( withPrefix( message ), throwable );
    }

    @Override
    public void log( @Nonnull String format, @Nullable Object... arguments )
    {
        delegate.get().log( withPrefix( format ), arguments );
    }

    @Override
    public void bulk( @Nonnull Consumer<Logger> consumer )
    {
        delegate.get().bulk( logger -> consumer.accept( new DatabaseLogger( logContext, () -> logger ) ) );
    }

    private String withPrefix( String message )
    {
        if ( logContext != null )
        {
            return logContext.formatMessage( message );
        }
        return message;
    }
}
