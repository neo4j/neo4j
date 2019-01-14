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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;

/**
 * The CommandLocator locates named commands for the AdminTool, or supplies the set of available commands for printing
 * help output.
 */
public interface CommandLocator
{
    /**
     * Find a command provider that matches the given key or name, or throws {@link NoSuchElementException} if no
     * matching provider was found.
     * @param name The name of the provider to look for.
     * @return Any matching command provider.
     */
    AdminCommand.Provider findProvider( String name ) throws NoSuchElementException;

    /**
     * Get an iterable of all of the command providers that are available through this command locator instance.
     * @return An iterable of command providers.
     */
    Iterable<AdminCommand.Provider> getAllProviders();

    /**
     * Get a command locator that uses the {@link Service service locator} mechanism to find providers by their service
     * key.
     * @return A service locator based command locator.
     */
    static CommandLocator fromServiceLocator()
    {
        return new CommandLocator()
        {
            @Override
            public AdminCommand.Provider findProvider( String name )
            {
                return Service.load( AdminCommand.Provider.class, name );
            }

            @Override
            public Iterable<AdminCommand.Provider> getAllProviders()
            {
                return Service.load( AdminCommand.Provider.class );
            }
        };
    }

    /**
     * Augment the given command locator such that it also considers the command provider given through the supplier.
     * @param command A supplier of an additional command. Note that this may be called multiple times.
     * @param commands The command locator to augment with the additional command provider.
     * @return The augmented command locator.
     */
    static CommandLocator withAdditionalCommand( Supplier<AdminCommand.Provider> command, CommandLocator commands )
    {
        return new CommandLocator()
        {
            @Override
            public AdminCommand.Provider findProvider( String name )
            {
                AdminCommand.Provider provider = command.get();
                return Objects.equals( name, provider.name() ) ? provider : commands.findProvider( name );
            }

            @Override
            public Iterable<AdminCommand.Provider> getAllProviders()
            {
                return Iterables.append( command.get(), commands.getAllProviders() );
            }
        };
    }
}
