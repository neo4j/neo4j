/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import java.nio.file.Path;
import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

public class UnbindFromClusterCommandProvider extends AdminCommand.Provider
{
    public UnbindFromClusterCommandProvider()
    {
        super( "unbind" );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return UnbindFromClusterCommand.arguments();
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Removes cluster state data for the specified database.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return ClusteringCommandSection.instance();
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Removes cluster state data for the specified database, so that the " +
               "instance can rebind to a new or recovered cluster.";
    }

    @Override
    @Nonnull
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new UnbindFromClusterCommand( homeDir, configDir, outsideWorld );
    }
}
