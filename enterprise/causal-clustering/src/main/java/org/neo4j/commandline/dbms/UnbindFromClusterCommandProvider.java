/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
