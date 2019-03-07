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

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.annotations.service.Service;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.service.NamedService;

import static java.lang.String.format;

/**
 * To create a command for {@code neo4j-admin}:
 * <ol>
 *   <li>implement {@code AdminCommand}</li>
 *   <li>implement {@code AdminCommand.Provider} which instantiates the command;
 *   annotate it with {@link ServiceProvider}, make {@link Provider#getName()} return command name
 * </ol>
 */
public interface AdminCommand
{
    void execute( String[] args ) throws IncorrectUsage, CommandFailed;

    @Service
    interface Provider extends NamedService
    {
        /**
         * @return The command's name
         */
        @Override
        @Nonnull
        String getName();

        /**
         * @return The arguments this command accepts.
         */
        @Nonnull
        Arguments allArguments();

        /**
         *
         * @return A list of possibly mutually-exclusive argument sets for this command.
         */
        @Nonnull
        default List<Arguments> possibleArguments()
        {
            return Collections.singletonList( allArguments() );
        }

        /**
         * @return A single-line summary for the command. Should be 70 characters or less.
         */
        @Nonnull
        String summary();

        /**
         * @return AdminCommandSection the command using the provider is grouped under
         */
        @Nonnull
        AdminCommandSection commandSection();

        /**
         * @return A description for the command's help text.
         */
        @Nonnull
        String description();

        @Nonnull
        AdminCommand create( CommandContext ctx );

        default void printSummary( Consumer<String> output )
        {
            output.accept( format( "%s", getName() ) );
            output.accept( "    " + summary() );
        }
    }
}
