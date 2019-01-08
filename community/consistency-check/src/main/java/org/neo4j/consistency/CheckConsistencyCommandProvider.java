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
package org.neo4j.consistency;

import java.nio.file.Path;
import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

import static java.lang.String.format;

public class CheckConsistencyCommandProvider extends AdminCommand.Provider
{
    public CheckConsistencyCommandProvider()
    {
        super( "check-consistency" );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return CheckConsistencyCommand.arguments();
    }

    @Override
    @Nonnull
    public String description()
    {
        return format(
                "This command allows for checking the consistency of a database or a backup thereof. It cannot " +
                        "be used with a database which is currently in use.%n" +
                        "%n" +
                        "All checks except 'check-graph' can be quite expensive so it may be useful to turn them off" +
                        " for very large databases. Increasing the heap size can also be a good idea." +
                        " See 'neo4j-admin help' for details." );
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Check the consistency of a database.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return AdminCommandSection.general();
    }

    @Override
    @Nonnull
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new CheckConsistencyCommand( homeDir, configDir, outsideWorld );
    }
}
