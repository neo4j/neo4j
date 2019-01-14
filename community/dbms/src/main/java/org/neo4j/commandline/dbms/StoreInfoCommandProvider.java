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
package org.neo4j.commandline.dbms;

import java.nio.file.Path;
import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

public class StoreInfoCommandProvider extends AdminCommand.Provider
{
    public StoreInfoCommandProvider()
    {
        super( "store-info" );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return StoreInfoCommand.arguments();
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Prints information about a Neo4j database store.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return AdminCommandSection.general();
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Prints information about a Neo4j database store, such as what version of Neo4j created it. Note that " +
                "this command expects a path to a store directory, for example --store=data/databases/graph.db.";
    }

    @Override
    @Nonnull
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new StoreInfoCommand( outsideWorld::stdOutLine );
    }
}
