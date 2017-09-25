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
package org.neo4j.restore;

import java.io.File;
import java.io.IOException;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Validators;

import static java.lang.String.format;
import static org.neo4j.commandline.Util.checkLock;
import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;

public class RestoreDatabaseCommand
{
    private FileSystemAbstraction fs;
    private final File fromPath;
    private final File databaseDir;
    private String databaseName;
    private boolean forceOverwrite;

    public RestoreDatabaseCommand( FileSystemAbstraction fs, File fromPath, Config config,
                                   String databaseName, boolean forceOverwrite )
    {
        this.fs = fs;
        this.fromPath = fromPath;
        this.databaseName = databaseName;
        this.forceOverwrite = forceOverwrite;
        this.databaseDir = config.get( database_path ).getAbsoluteFile();
    }

    public void execute() throws IOException, CommandFailed
    {
        if ( !fs.fileExists( fromPath ) )
        {
            throw new IllegalArgumentException( format( "Source directory does not exist [%s]", fromPath ) );
        }

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( fromPath );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException(
                    format( "Source directory is not a database backup [%s]", fromPath ) );
        }

        if ( fs.fileExists( databaseDir ) && !forceOverwrite )
        {
            throw new IllegalArgumentException( format( "Database with name [%s] already exists at %s",
                    databaseName, databaseDir ) );
        }

        checkLock( databaseDir.toPath() );

        fs.deleteRecursively( databaseDir );
        fs.copyRecursively( fromPath, databaseDir );
    }
}
