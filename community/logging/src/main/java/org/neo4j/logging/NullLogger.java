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

/**
 * A {@link Logger} implementation that discards all messages
 */
public final class NullLogger implements Logger
{
    private static final NullLogger INSTANCE = new NullLogger();

    private NullLogger()
    {
    }

    /**
     * @return A singleton {@link NullLogger} instance
     */
    public static NullLogger getInstance()
    {
        return INSTANCE;
    }

    @Override
    public void log( @Nonnull String message )
    {
    }

    @Override
    public void log( @Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void log( @Nonnull String format, @Nonnull Object... arguments )
    {
    }

    @Override
    public void bulk( @Nonnull Consumer<Logger> consumer )
    {
    }
}
