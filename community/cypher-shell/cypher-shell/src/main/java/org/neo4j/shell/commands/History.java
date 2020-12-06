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
package org.neo4j.shell.commands;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;

import static org.neo4j.shell.commands.CommandHelper.simpleArgParse;

/**
 * Show command history
 */
public class History implements Command
{
    private static final String COMMAND_NAME = ":history";

    private final Logger logger;
    private final Historian historian;
    private final List<String> aliases = Collections.emptyList();

    public History( @Nonnull final Logger logger, @Nonnull final Historian historian )
    {
        this.logger = logger;
        this.historian = historian;
    }

    @Nonnull
    @Override
    public String getName()
    {
        return COMMAND_NAME;
    }

    @Nonnull
    @Override
    public String getDescription()
    {
        return "Print a list of the last commands executed";
    }

    @Nonnull
    @Override
    public String getUsage()
    {
        return "";
    }

    @Nonnull
    @Override
    public String getHelp()
    {
        return "Print a list of the last commands executed.";
    }

    @Nonnull
    @Override
    public List<String> getAliases()
    {
        return aliases;
    }

    @Override
    public void execute( @Nonnull String argString ) throws ExitException, CommandException
    {
        simpleArgParse( argString, 0, COMMAND_NAME, getUsage() );

        // Calculate starting position
        int lineCount = 16;

        logger.printOut( printHistory( historian.getHistory(), lineCount ) );
    }

    /**
     * Prints N last lines of history.
     *
     * @param lineCount number of entries to print
     */
    private String printHistory( @Nonnull final List<String> history, final int lineCount )
    {
        // for alignment, check the string length of history size
        int colWidth = Integer.toString( history.size() ).length();
        String fmt = " %-" + colWidth + "d  %s\n";

        String result = "";
        int count = 0;

        for ( int i = history.size() - 1; i >= 0 && count < lineCount; i--, count++ )
        {
            String line = history.get( i );
            // Executing old commands with !N actually starts from 1, and not 0, hence increment index by one
            result = String.format( fmt, i + 1, line ) + result;
        }

        return result;
    }
}
