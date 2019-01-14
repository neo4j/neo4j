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

    public void printUsageForCommand( AdminCommand.Provider command, Consumer<String> output )
    {
        final CommandUsage commandUsage = new CommandUsage( command, scriptName );
        commandUsage.printDetailed( output );
    }

    public void print( Consumer<String> output )
    {
        output.accept( format( "usage: %s <command>", scriptName ) );
        output.accept( "" );
        output.accept( "Manage your Neo4j instance." );
        output.accept( "" );

        printEnvironmentVariables( output );

        output.accept( "available commands:" );
        printCommands( output );

        output.accept( "" );
        output.accept( format( "Use %s help <command> for more details.", scriptName ) );
    }

    static void printEnvironmentVariables( Consumer<String> output )
    {
        output.accept( "environment variables:" );
        output.accept( "    NEO4J_CONF    Path to directory which contains neo4j.conf." );
        output.accept( "    NEO4J_DEBUG   Set to anything to enable debug output." );
        output.accept( "    NEO4J_HOME    Neo4j home directory." );
        output.accept( "    HEAP_SIZE     Set JVM maximum heap size during command execution." );
        output.accept( "                  Takes a number and a unit, for example 512m." );
        output.accept( "" );
    }

    private void printCommands( Consumer<String> output )
    {
        Map<AdminCommandSection,List<AdminCommand.Provider>> groupedProviders = groupProvidersBySection();

        AdminCommandSection.general()
                .printAllCommandsUnderSection( output, groupedProviders.remove( AdminCommandSection.general() ) );

        groupedProviders.entrySet().stream()
                .sorted( Comparator.comparing( groupedProvider -> groupedProvider.getKey().printable() ) )
                .forEach(entry -> entry.getKey().printAllCommandsUnderSection( output, entry.getValue() ) );
    }

    private Map<AdminCommandSection,List<AdminCommand.Provider>> groupProvidersBySection()
    {
        List<AdminCommand.Provider> providers = new ArrayList<>();
        commands.getAllProviders().forEach( providers::add );
        return providers.stream().collect( Collectors.groupingBy( AdminCommand.Provider::commandSection ) );
    }
}
