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
package org.neo4j.commandline.admin.security;

import javax.annotation.Nonnull;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.CommandContext;
import org.neo4j.commandline.arguments.Arguments;

@ServiceProvider
public class SetDefaultAdminCommandProvider implements AdminCommand.Provider
{
    @Nonnull
    @Override
    public String getName()
    {
        return SetDefaultAdminCommand.COMMAND_NAME;
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return SetDefaultAdminCommand.arguments();
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Sets the user to become admin if users but no roles are present, " +
                "for example when upgrading to neo4j 3.1 enterprise.";
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Sets the default admin user when no roles are present.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return AuthenticationCommandSection.instance();
    }

    @Override
    @Nonnull
    public AdminCommand create( CommandContext ctx )
    {
        return new SetDefaultAdminCommand( ctx.getHomeDir(), ctx.getConfigDir(), ctx.getOutsideWorld() );
    }
}
