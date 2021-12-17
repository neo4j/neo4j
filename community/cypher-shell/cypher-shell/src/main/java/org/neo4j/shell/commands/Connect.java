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
package org.neo4j.shell.commands;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import java.util.Collections;
import java.util.List;

import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.terminal.CypherShellTerminal;

import static java.lang.String.format;
import static org.neo4j.shell.commands.CommandHelper.simpleArgParse;

/**
 * Connects to a database
 */
public class Connect implements Command
{
    private static final String COMMAND_NAME = ":connect";

    private final CypherShell shell;
    private final CypherShellTerminal terminal;
    private final ConnectionConfig config;
    private final ArgumentParser argumentParser;

    public Connect( CypherShell shell, CypherShellTerminal terminal, ConnectionConfig config )
    {
        this.shell = shell;
        this.terminal = terminal;
        this.config = config;
        this.argumentParser = setupParser();
    }

    @Override
    public String getName()
    {
        return COMMAND_NAME;
    }

    @Override
    public String getDescription()
    {
        return "Connects to a database";
    }

    @Override
    public String getUsage()
    {
        return "[-u USERNAME, --username USERNAME], [-p PASSWORD, --password PASSWORD], [-d DATABASE, --database DATABASE]";
    }

    @Override
    public String getHelp()
    {
        return format( ":connect %s, connects to a database", getUsage() );
    }

    @Override
    public List<String> getAliases()
    {
        return Collections.emptyList();
    }

    @Override
    public void execute( final String argString ) throws ExitException, CommandException
    {
        if ( shell.isConnected() )
        {
            throw new CommandException( "Already connected" );
        }

        parseArgString( argString );
        shell.connect( config );
    }

    private void parseArgString( String argString ) throws CommandException
    {
        try
        {
            var args = argumentParser.parseArgs( simpleArgParse( argString, 0, 6, COMMAND_NAME, getUsage() ) );
            var user = args.getString( "username" );
            var password = args.getString( "password" );

            if ( user == null && password != null )
            {
                throw new CommandException( "You cannot provide password only, please provide a username using '-u USERNAME'" );
            }
            else if ( user == null ) // We know password is null because of the previous if statement
            {
                config.setUsername( promptForNonEmptyText( "username", null ) );
                config.setPassword( promptForText( "password", '*' ) );
            }
            else if ( password == null )
            {
                config.setUsername( user );
                config.setPassword( promptForText( "password", '*' ) );
            }
            else
            {
                config.setUsername( user );
                config.setPassword( password );
            }

            config.setDatabase( args.getString( "database" ) );
        }
        catch ( ArgumentParserException e )
        {
            throw new CommandException( format( "Invalid input string: '%s', usage: ':connect %s'", argString, getUsage() ) );
        }
    }

    private ArgumentParser setupParser()
    {
        var parser = ArgumentParsers.newFor( COMMAND_NAME ).build();
        parser.addArgument( "-d", "--database" ).setDefault( "" );
        parser.addArgument( "-u", "--username" );
        parser.addArgument( "-p", "--password" );
        return parser;
    }

    private String promptForNonEmptyText( String prompt, Character mask ) throws CommandException
    {
        String text = promptForText( prompt, mask );
        if ( !text.isEmpty() )
        {
            return text;
        }
        terminal.write().println( prompt + " cannot be empty" );
        terminal.write().println();
        return promptForNonEmptyText( prompt, mask );
    }

    private String promptForText( String prompt, Character mask ) throws CommandException
    {
        try
        {
            return terminal.read().simplePrompt( prompt + ": ", mask );
        }
        catch ( NoMoreInputException | UserInterruptException e )
        {
            throw new CommandException( "No text could be read, exiting..." );
        }
    }
}
