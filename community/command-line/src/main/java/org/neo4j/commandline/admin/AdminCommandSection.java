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

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

public abstract class AdminCommandSection
{
    private static final AdminCommandSection GENERAL = new GeneralSection();
    public abstract String printable();

    public static AdminCommandSection general()
    {
        return GENERAL;
    }

    void printAllCommandsUnderSection( Consumer<String> output, List<AdminCommand.Provider> providers,
            String scriptName )
    {
        output.accept( printable() );
        providers.sort( Comparator.comparing( AdminCommand.Provider::name ) );
        providers.forEach( command ->
        {
            final CommandUsage commandUsage = new CommandUsage( command, scriptName );
            commandUsage.printIndentedSummary( output );
        } );
    }

    static class GeneralSection extends AdminCommandSection
    {
        @Override
        public String printable()
        {
            return "General";
        }
    }
}
