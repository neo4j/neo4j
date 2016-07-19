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

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;

/**
 * The CommandLocator locates named commands for the AdminTool, or supplies the set of available commands for printing
 * help output.
 */
public interface CommandLocator
        extends Function<String, AdminCommand.Provider>, Supplier<Iterable<AdminCommand.Provider>>
{

    static CommandLocator fromServiceLocator()
    {
        return new CommandLocator()
        {
            @Override
            public AdminCommand.Provider apply( String name )
            {
                return Service.load( AdminCommand.Provider.class, name );
            }

            @Override
            public Iterable<AdminCommand.Provider> get()
            {
                return Service.load( AdminCommand.Provider.class );
            }
        };
    }

    static CommandLocator withAdditionalCommand( Supplier<AdminCommand.Provider> command, CommandLocator commands )
    {
        return new CommandLocator()
        {
            @Override
            public AdminCommand.Provider apply( String name )
            {
                AdminCommand.Provider provider = command.get();
                return Objects.equals( name, provider.name() ) ? provider : commands.apply( name );
            }

            @Override
            public Iterable<AdminCommand.Provider> get()
            {
                return Iterables.append( command.get(), commands.get() );
            }
        };
    }
}
