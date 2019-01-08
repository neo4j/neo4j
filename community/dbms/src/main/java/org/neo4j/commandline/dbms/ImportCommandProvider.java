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
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

public class ImportCommandProvider extends AdminCommand.Provider
{
    public ImportCommandProvider()
    {
        super( "import" );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return ImportCommand.allArguments();
    }

    @Override
    @Nonnull
    public List<Arguments> possibleArguments()
    {
        return Arrays.asList( ImportCommand.csvArguments(), ImportCommand.databaseArguments() );
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Import a collection of CSV files with --mode=csv (default), or a database from " +
                "a pre-3.0 installation with --mode=database.";
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Import from a collection of CSV files or a pre-3.0 database.";
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
        return new ImportCommand( homeDir, configDir, outsideWorld );
    }
}
