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
package org.neo4j.commandline.dbms;

import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

public class VersionCommandProvider extends AdminCommand.Provider
{

    public VersionCommandProvider()
    {
        super( "version" );
    }

    @Override
    public Arguments allArguments()
    {
        return VersionCommand.arguments();
    }

    @Override
    public String summary()
    {
        return "Check the version of a Neo4j database store.";
    }

    @Override
    public String description()
    {
        return "Checks the version of a Neo4j database store. Note that this command expects a path to a store " +
                "directory, for example --store=data/databases/graph.db.";
    }

    @Override
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new VersionCommand( outsideWorld::stdOutLine );
    }
}
