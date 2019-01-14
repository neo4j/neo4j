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
package org.neo4j.logging.slf4j;

import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

/**
 * An adapter from a {@link org.slf4j.Logger} to a {@link Log} interface
 */
public class Slf4jLog extends AbstractLog
{
    private final Object lock;
    private final org.slf4j.Logger slf4jLogger;
    private final Logger debugLogger;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    /**
     * @param slf4jLogger the SLF4J logger to output to
     */
    public Slf4jLog( final org.slf4j.Logger slf4jLogger )
    {
        this.lock = this;
        this.slf4jLogger = slf4jLogger;

        this.debugLogger = new Logger()
        {
            @Override
            public void log( @Nonnull String message )
            {
                synchronized ( lock )
                {
                    slf4jLogger.debug( message );
                }
            }

            @Override
            public void log( @Nonnull String message, @Nonnull Throwable throwable )
            {
                synchronized ( lock )
                {
                    slf4jLogger.debug( message, throwable );
                }
            }

            @Override
            public void log( @Nonnull String format, @Nonnull Object... arguments )
            {
                synchronized ( lock )
                {
                    slf4jLogger.debug( convertFormat(format), arguments );
                }
            }

            @Override
            public void bulk( @Nonnull Consumer<Logger> consumer )
            {
                synchronized ( lock )
                {
                    consumer.accept( this );
                }
            }
        };

        this.infoLogger = new Logger()
        {
            @Override
            public void log( @Nonnull String message )
            {
                synchronized ( lock )
                {
                    slf4jLogger.info( message );
                }
            }

            @Override
            public void log( @Nonnull String message, @Nonnull Throwable throwable )
            {
                synchronized ( lock )
                {
                    slf4jLogger.info( message, throwable );
                }
            }

            @Override
            public void log( @Nonnull String format, @Nonnull Object... arguments )
            {
                synchronized ( lock )
                {
                    slf4jLogger.info( convertFormat(format), arguments );
                }
            }

            @Override
            public void bulk( @Nonnull Consumer<Logger> consumer )
            {
                synchronized ( lock )
                {
                    consumer.accept( this );
                }
            }
        };

        this.warnLogger = new Logger()
        {
            @Override
            public void log( @Nonnull String message )
            {
                synchronized ( lock )
                {
                    slf4jLogger.warn( message );
                }
            }

            @Override
            public void log( @Nonnull String message, @Nonnull Throwable throwable )
            {
                synchronized ( lock )
                {
                    slf4jLogger.warn( message, throwable );
                }
            }

            @Override
            public void log( @Nonnull String format, @Nonnull Object... arguments )
            {
                synchronized ( lock )
                {
                    slf4jLogger.warn( convertFormat(format), arguments );
                }
            }

            @Override
            public void bulk( @Nonnull Consumer<Logger> consumer )
            {
                synchronized ( lock )
                {
                    consumer.accept( this );
                }
            }
        };

        this.errorLogger = new Logger()
        {
            @Override
            public void log( @Nonnull String message )
            {
                synchronized ( lock )
                {
                    slf4jLogger.error( message );
                }
            }

            @Override
            public void log( @Nonnull String message, @Nonnull Throwable throwable )
            {
                synchronized ( lock )
                {
                    slf4jLogger.error( message, throwable );
                }
            }

            @Override
            public void log( @Nonnull String format, @Nonnull Object... arguments )
            {
                synchronized ( lock )
                {
                    slf4jLogger.error( convertFormat(format), arguments );
                }
            }

            @Override
            public void bulk( @Nonnull Consumer<Logger> consumer )
            {
                synchronized ( lock )
                {
                    consumer.accept( this );
                }
            }
        };
    }

    @Override
    public boolean isDebugEnabled()
    {
        return slf4jLogger.isDebugEnabled();
    }

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return this.debugLogger;
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return this.infoLogger;
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return this.warnLogger;
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return this.errorLogger;
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        synchronized ( lock )
        {
            consumer.accept( this );
        }
    }

    private String convertFormat( String format )
    {
        return format.replace( "%s", "{}" );
    }
}
