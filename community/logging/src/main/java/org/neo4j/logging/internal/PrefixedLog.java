/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;

import static java.util.Objects.requireNonNull;

public class PrefixedLog extends AbstractLog
{
    private final String prefix;
    private final Log delegate;

    PrefixedLog( String prefix, Log delegate )
    {
        requireNonNull( prefix, "prefix must be a string" );
        requireNonNull( delegate, "delegate log cannot be null" );
        this.prefix = "[" + prefix + "] ";
        this.delegate = delegate;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return delegate.isDebugEnabled();
    }

    @Override
    public void debug( @Nonnull String message )
    {
        delegate.debug( withPrefix( message ));
    }

    @Override
    public void debug( @Nonnull String message, @Nonnull Throwable throwable )
    {
        delegate.debug( withPrefix( message ), throwable );
    }

    @Override
    public void debug( @Nonnull String format, @Nullable Object... arguments )
    {
        delegate.debug( withPrefix( format ), arguments );
    }

    @Override
    public void info( @Nonnull String message )
    {
        delegate.info( withPrefix( message ) );
    }

    @Override
    public void info( @Nonnull String message, @Nonnull Throwable throwable )
    {
        delegate.info( withPrefix( message ), throwable );
    }

    @Override
    public void info( @Nonnull String format, @Nullable Object... arguments )
    {
        delegate.info( withPrefix( format ), arguments );
    }

    @Override
    public void warn( @Nonnull String message )
    {
        delegate.warn( withPrefix( message ) );
    }

    @Override
    public void warn( @Nonnull String message, @Nonnull Throwable throwable )
    {
        delegate.warn( withPrefix( message ), throwable );
    }

    @Override
    public void warn( @Nonnull String format, @Nullable Object... arguments )
    {
        delegate.warn( withPrefix( format ), arguments );
    }

    @Override
    public void error( @Nonnull String message )
    {
        delegate.error( withPrefix( message ) );
    }

    @Override
    public void error( @Nonnull String message, @Nonnull Throwable throwable )
    {
        delegate.error( withPrefix( message ), throwable );
    }

    @Override
    public void error( @Nonnull String format, @Nullable Object... arguments )
    {
        delegate.error( withPrefix( format ), arguments );
    }

    @Override
    @Deprecated
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        delegate.bulk( log -> consumer.accept( this ) );
    }

    private String withPrefix( String message )
    {
        return prefix + message;
    }
}
