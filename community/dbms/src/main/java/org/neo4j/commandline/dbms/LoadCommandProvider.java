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
import org.neo4j.dbms.archive.Loader;

public class LoadCommandProvider extends AdminCommand.Provider
{
    public LoadCommandProvider()
    {
        super( "load" );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return LoadCommand.arguments();
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Load a database from an archive. <archive-path> must be an archive created with the dump " +
                "command. <database> is the name of the database to create. Existing databases can be replaced " +
                "by specifying --force. It is not possible to replace a database that is mounted in a running " +
                "Neo4j server.";
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Load a database from an archive created with the dump command.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return OfflineBackupCommandSection.instance();
    }

    @Override
    @Nonnull
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new LoadCommand( homeDir, configDir, new Loader() );
    }
}
