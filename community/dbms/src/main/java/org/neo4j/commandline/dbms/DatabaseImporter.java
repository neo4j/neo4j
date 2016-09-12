/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validators;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class DatabaseImporter
{
    private final String database;
    private final File from;
    private final Config config;

    public DatabaseImporter( Args parsedArgs, Config config ) throws IncorrectUsage
    {
        try
        {
            this.database = parsedArgs.interpretOption( "database", Converters.<String>mandatory(), s -> s );
            this.from = parsedArgs.interpretOption( "from", Converters.<File>mandatory(), Converters.toFile(),
                    Validators.CONTAINS_EXISTING_DATABASE );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        this.config =
                config.with( stringMap( DatabaseManagementSystemSettings.active_database.name(), this.database ) );
    }

    public void doImport() throws IOException
    {
        copyDatabase( from, config );
        removeMessagesLog( config );
    }

    private void copyDatabase( File from, Config config ) throws IOException
    {
        FileUtils.copyRecursively( from, config.get( database_path ) );
    }

    private void removeMessagesLog( Config config )
    {
        FileUtils.deleteFile( new File( config.get( database_path ), "messages.log" ) );
    }
}
