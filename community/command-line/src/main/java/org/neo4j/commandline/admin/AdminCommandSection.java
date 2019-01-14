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

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

public abstract class AdminCommandSection
{
    private static final AdminCommandSection GENERAL = new GeneralSection();

    @Nonnull
    public abstract String printable();

    public static AdminCommandSection general()
    {
        return GENERAL;
    }

    @Override
    public int hashCode()
    {
        return this.printable().hashCode();
    }

    @Override
    public boolean equals( Object other )
    {
        return other instanceof AdminCommandSection &&
                this.printable().equals( ((AdminCommandSection) other).printable() );
    }

    public final void printAllCommandsUnderSection( Consumer<String> output, List<AdminCommand.Provider> providers )
    {
        output.accept( "" );
        output.accept( printable() );
        providers.sort( Comparator.comparing( AdminCommand.Provider::name ) );
        providers.forEach( provider -> provider.printSummary( s -> output.accept( "    " + s ) ) );
    }

    static class GeneralSection extends AdminCommandSection
    {
        @Override
        @Nonnull
        public String printable()
        {
            return "General";
        }
    }
}
