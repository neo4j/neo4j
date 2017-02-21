/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.commandline.admin;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Usage
{
    private final String scriptName;
    private final CommandLocator commands;

    public Usage( String scriptName, CommandLocator commands )
    {
        this.scriptName = scriptName;
        this.commands = commands;
    }

    public void print( Consumer<String> output )
    {
        output.accept( format( "usage: %s <command>", scriptName ) );
        output.accept( "" );
        output.accept( "available commands:" );

        Map<AdminCommandSection,List<AdminCommand.Provider>> groupedProviders = groupProvidersBySegment();

        printAllCommandsUnderSection( output, AdminCommandSection.general(),
                groupedProviders.remove( AdminCommandSection.general() ) );

        groupedProviders.entrySet().stream()
                .sorted( Comparator.comparing( groupedProvider -> groupedProvider.getKey().printable() ) )
                .forEach( entry -> printAllCommandsUnderSection( output, entry.getKey(), entry.getValue() ) );

        output.accept( "" );
        output.accept( format( "Use %s help <command> for more details.", scriptName ) );
    }

    private Map<AdminCommandSection,List<AdminCommand.Provider>> groupProvidersBySegment()
    {
        List<AdminCommand.Provider> providers = new ArrayList<>();
        commands.getAllProviders().forEach( providers::add );
        return providers.stream().collect( Collectors.groupingBy( AdminCommand.Provider::commandSection ) );
    }

    private void printAllCommandsUnderSection( Consumer<String> output, AdminCommandSection section,
            List<AdminCommand.Provider> providers )
    {
        output.accept( section.printable() );
        providers.sort( Comparator.comparing( AdminCommand.Provider::name ) );
        providers.forEach( command ->
        {
            final CommandUsage commandUsage = new CommandUsage( command, scriptName );
            commandUsage.printIndentedSummary( output );
        } );
    }

    public void printUsageForCommand( AdminCommand.Provider command, Consumer<String> output )
    {
        final CommandUsage commandUsage = new CommandUsage( command, scriptName );
        commandUsage.printDetailed( output );
    }
}
