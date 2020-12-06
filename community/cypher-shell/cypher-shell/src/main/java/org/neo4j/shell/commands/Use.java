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

import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;

import static org.neo4j.shell.commands.CommandHelper.simpleArgParse;

/**
 * This command starts a transaction.
 */
public class Use implements Command
{
    private static final String COMMAND_NAME = ":use";
    @Nonnull
    private final DatabaseManager databaseManager;
    @Nonnull
    private String databaseName;

    public Use( @Nonnull final DatabaseManager databaseManager )
    {
        this.databaseManager = databaseManager;
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
        return "Set the active database";
    }

    @Nonnull
    @Override
    public String getUsage()
    {
        return "database";
    }

    @Nonnull
    @Override
    public String getHelp()
    {
        return String.format( "Set the active database that transactions are executed on", Commit.COMMAND_NAME );
    }

    @Nonnull
    @Override
    public List<String> getAliases()
    {
        return Collections.emptyList();
    }

    @Override
    public void execute( @Nonnull final String argString ) throws ExitException, CommandException
    {
        String[] args = simpleArgParse( argString, 0, 1, COMMAND_NAME, getUsage() );
        String databaseName = args.length == 0 ? DatabaseManager.ABSENT_DB_NAME : args[0];
        databaseManager.setActiveDatabase( databaseName );
    }
}
