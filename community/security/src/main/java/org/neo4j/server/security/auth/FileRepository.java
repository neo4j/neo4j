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
package org.neo4j.server.security.auth;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

public interface FileRepository
{
    /**
     * This is used by different flavors of file repositories to agree on a naming convention
     * for repository files that are renamed after migration to prevent accidental reuse.
     */
    static File getMigratedFile( File file )
    {
        return new File( file.getParent(), file.getName() + ".migrated" );
    }

    static void assertNotMigrated( File file, FileSystemAbstraction fileSystem, Log log )
    {
        File migratedFile = getMigratedFile( file );
        if ( fileSystem.fileExists( migratedFile ) )
        {
            String message = "The repository file '" + file.getAbsolutePath() + "' has been marked as migrated. " +
                    "If you are sure that you want use this repository you need to manually rename the file '" +
                    migratedFile.getAbsolutePath() + "' to '" + file.getName() + "'";
            log.error( message );
            throw new IllegalStateException( message );
        }
    }
}
