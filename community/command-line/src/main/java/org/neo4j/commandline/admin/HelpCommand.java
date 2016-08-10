/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Consumer;

public class HelpCommand implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {
        private final Usage usage;

        public Provider( Usage usage )
        {
            super( "help" );
            this.usage = usage;
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.empty();
        }

        @Override
        public String description()
        {
            return "Display this help text.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new HelpCommand( usage, outsideWorld::stdOutLine );
        }
    }

    private final Usage usage;
    private final Consumer<String> output;

    public HelpCommand( Usage usage, Consumer<String> output )
    {
        this.usage = usage;
        this.output = output;
    }

    @Override
    public void execute( String... args )
    {
        usage.print( output );
    }
}
