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
package org.neo4j.commandline.admin.security;

import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;

public class SetInitialPasswordCommandProvider extends AdminCommand.Provider
{

    public SetInitialPasswordCommandProvider()
    {
        super( "set-initial-password" );
    }

    @Override
    public Arguments allArguments()
    {
        return SetInitialPasswordCommand.arguments();
    }

    @Override
    public String description()
    {
        return "Sets the initial password of the initial admin user ('" + INITIAL_USER_NAME + "').";
    }

    @Override
    public String summary()
    {
        return description();
    }

    @Override
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new SetInitialPasswordCommand( homeDir, configDir, outsideWorld );
    }
}
