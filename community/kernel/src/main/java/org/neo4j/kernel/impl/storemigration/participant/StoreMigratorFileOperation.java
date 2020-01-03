/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.storemigration.participant;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.FileOperation;

import static org.neo4j.stream.Streams.ofOptional;

class StoreMigratorFileOperation
{
    /**
     * Performs a file operation on a database's store files from one directory
     * to another. Remember that in the case of {@link FileOperation#MOVE moving files}, the way that's done is to
     * just rename files (the standard way of moving with JDK6) from and to must be on the same disk partition.
     *
     * @param fromLayout directory that hosts the database files.
     * @param toLayout directory to receive the database files.
     * @throws IOException if any of the operations fail for any reason.
     */
    static void fileOperation( FileOperation operation, FileSystemAbstraction fs, DatabaseLayout fromLayout, DatabaseLayout toLayout,
            Iterable<DatabaseFile> databaseFiles, boolean allowSkipNonExistentFiles, ExistingTargetStrategy existingTargetStrategy ) throws IOException
    {
        for ( DatabaseFile databaseStore : databaseFiles )
        {
            File[] files = Stream.concat( fromLayout.file( databaseStore ), ofOptional( fromLayout.idFile( databaseStore ) ) ).toArray( File[]::new );
            perform( operation, fs, fromLayout, toLayout, allowSkipNonExistentFiles, existingTargetStrategy, files );
        }
    }

    private static void perform( FileOperation operation, FileSystemAbstraction fs, DatabaseLayout fromLayout, DatabaseLayout toLayout,
            boolean allowSkipNonExistentFiles, ExistingTargetStrategy existingTargetStrategy, File[] files ) throws IOException
    {
        for ( File file : files )
        {
            if ( file != null )
            {
                operation.perform( fs, file.getName(), fromLayout.databaseDirectory(), allowSkipNonExistentFiles, toLayout.databaseDirectory(),
                        existingTargetStrategy );
            }
        }
    }
}
