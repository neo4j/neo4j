/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.admin;

import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

class AugmentedCommandLocator
{
    /**
     * Useful for tests where you know the list of providers but don't want to use the service locator
     *
     * @param providers list of command providers that the command locator will use
     * @return a service locator based command locator that uses provided list of commands
     */
    static CommandLocator fromFixedArray( AdminCommand.Provider... providers )
    {
        return new CommandLocator()
        {
            Collection<AdminCommand.Provider> providerCollection = Arrays.asList( providers );

            @Override
            public AdminCommand.Provider findProvider( String name ) throws NoSuchElementException
            {
                return providerCollection.stream().filter( provider -> name.equals( provider.name() ) ).findFirst().orElseThrow( NoSuchElementException::new );
            }

            @Override
            public Iterable<AdminCommand.Provider> getAllProviders()
            {
                return providerCollection;
            }
        };
    }
}
