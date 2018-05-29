/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.restore;

import java.io.File;
import java.io.IOException;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

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
