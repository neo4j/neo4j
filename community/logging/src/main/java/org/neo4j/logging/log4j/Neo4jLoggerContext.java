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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.Closeable;

import org.neo4j.io.IOUtils;

/**
 * Facade for Log4j LoggerContext.
 */
public class Neo4jLoggerContext implements Closeable
{
    private final LoggerContext ctx;
    private final Closeable additionalClosable;

    public Neo4jLoggerContext( LoggerContext ctx, Closeable additionalClosable )
    {
        this.ctx = ctx;
        this.additionalClosable = additionalClosable;
    }

    /**
     * Package-private specifically to not leak {@link Logger} outside logging module.
     * Should not be used outside of the logging module.
     */
    Logger getLogger( String name )
    {
        return ctx.getLogger( name );
    }

    /**
     * Package-private specifically to not leak {@link LoggerContext} outside logging module.
     * Should not be used outside of the logging module.
     */
    LoggerContext getLoggerContext()
    {
        return ctx;
    }

    boolean haveExternalResources()
    {
        return additionalClosable != null;
    }

    @Override
    public void close()
    {
        LogManager.shutdown( ctx );
        if ( additionalClosable != null )
        {
            IOUtils.closeAllSilently( additionalClosable );
        }
    }
}
