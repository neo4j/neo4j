/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.logging.log4j.LogExtended;

public abstract class AbstractLog implements LogExtended
{
    @Override
    public Logger debugLogger()
    {
        return new Logger()
        {
            @Override
            public void log( String message )
            {
                debug( message );
            }

            @Override
            public void log( String message, Throwable throwable )
            {
                debug( message, throwable );
            }

            @Override
            public void log( String format, Object... arguments )
            {
                debug( format, arguments );
            }

            @Override
            public void bulk( Consumer<Logger> consumer )
            {
                consumer.accept( this );
            }
        };
    }

    @Override
    public Logger infoLogger()
    {
        return new Logger()
        {
            @Override
            public void log( String message )
            {
                info( message );
            }

            @Override
            public void log( String message, Throwable throwable )
            {
                info( message, throwable );
            }

            @Override
            public void log( String format, Object... arguments )
            {
                info( format, arguments );
            }

            @Override
            public void bulk( Consumer<Logger> consumer )
            {
                consumer.accept( this );
            }
        };
    }

    @Override
    public Logger warnLogger()
    {
        return new Logger()
        {
            @Override
            public void log( String message )
            {
                warn( message );
            }

            @Override
            public void log( String message, Throwable throwable )
            {
                warn( message, throwable );
            }

            @Override
            public void log( String format, Object... arguments )
            {
                warn( format, arguments );
            }

            @Override
            public void bulk( Consumer<Logger> consumer )
            {
                consumer.accept( this );
            }
        };
    }

    @Override
    public Logger errorLogger()
    {
        return new Logger()
        {
            @Override
            public void log( String message )
            {
                error( message );
            }

            @Override
            public void log( String message, Throwable throwable )
            {
                error( message, throwable );
            }

            @Override
            public void log( String format, Object... arguments )
            {
                error( format, arguments );
            }

            @Override
            public void bulk( Consumer<Logger> consumer )
            {
                consumer.accept( this );
            }
        };
    }

}
