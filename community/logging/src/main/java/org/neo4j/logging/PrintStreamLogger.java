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

import java.io.PrintStream;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

public class PrintStreamLogger implements Logger
{
    private PrintStream printStream;

    public PrintStreamLogger( PrintStream printStream )
    {
        this.printStream = printStream;
    }

    @Override
    public void log( @Nonnull String message )
    {
        printStream.println( message );
    }

    @Override
    public void log( @Nonnull String message, @Nonnull Throwable throwable )
    {
        printStream.printf( "%s, cause: %s%n", message, throwable );

    }

    @Override
    public void log( @Nonnull String format, @Nonnull Object... arguments )
    {
        printStream.printf( format, arguments );
        printStream.println();
    }

    @Override
    public void bulk( @Nonnull Consumer<Logger> consumer )
    {
        Objects.requireNonNull( consumer );
        consumer.accept( this );
    }
}
