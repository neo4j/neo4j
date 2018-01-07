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

import java.io.PrintStream;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

/**
 * A log that writes to a {@link PrintStream}.
 */
public class PrintStreamLog extends AbstractLog
{
    private final PrintStreamLogger logger;
    private final boolean debugEnabled;

    public static PrintStreamLog newStdOutLog()
    {
        return new PrintStreamLog( System.out, false );
    }

    public static PrintStreamLog newStdErrLog()
    {
        return new PrintStreamLog( System.err, false );
    }

    public PrintStreamLog( PrintStream target, boolean debugEnabled )
    {
        this.logger = new PrintStreamLogger( target );
        this.debugEnabled = debugEnabled;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return logger;
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return logger;
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return logger;
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return logger;
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        consumer.accept( this );
    }
}
