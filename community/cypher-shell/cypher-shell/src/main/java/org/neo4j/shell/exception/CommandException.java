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
package org.neo4j.shell.exception;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.shell.log.AnsiFormattedText;

/**
 * And exception indicating that a command invocation failed.
 */
public class CommandException extends AnsiFormattedException
{
    public CommandException( @Nullable String msg )
    {
        super( msg );
    }

    public CommandException( @Nullable String msg, Throwable cause )
    {
        super( msg, cause );
    }

    public CommandException( @Nonnull AnsiFormattedText append )
    {
        super( append );
    }
}
