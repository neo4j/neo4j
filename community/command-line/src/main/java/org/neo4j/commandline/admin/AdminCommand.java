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

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;

/**
 * To create a command for {@code neo4j-admin}:
 * <ol>
 *   <li>implement {@code AdminCommand}</li>
 *   <li>create a concrete subclass of {@code AdminCommand.Provider} which instantiates the command</li>
 *   <li>register the {@code Provider} in {@code META-INF/services} as described
 *     <a href='https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html'>here</a></li>
 * </ol>
 */
public interface AdminCommand
{
    abstract class Provider extends Service
    {
        /**
         * Create a new instance of a service implementation identified with the
         * specified key(s).
         *
         * @param key     the main key for identifying this service implementation
         * @param altKeys alternative spellings of the identifier of this service
         */
        protected Provider( String key, String... altKeys )
        {
            super( key, altKeys );
        }

        /**
         * @return The command's name
         */
        public String name()
        {
            return Iterables.last( getKeys() );
        }

        /**
         * @return A help string for the command's arguments, if any.
         */
        public abstract Optional<String> arguments();

        /**
         * @return A description for the command's help text.
         */
        public abstract String description();

        public abstract AdminCommand create( Path homeDir, Path configDir );
    }

    void execute( String[] args ) throws IncorrectUsage, CommandFailed;
}
