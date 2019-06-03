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
package org.neo4j.kernel.recovery;

import java.io.File;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;

class RecoveryStoreFileHelper
{
    private RecoveryStoreFileHelper()
    {
    }

    static boolean allIdFilesExist( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        return databaseLayout.idFiles().stream().allMatch( fileSystem::fileExists );
    }

    static boolean allStoreFilesExist( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        Set<File> storeFiles = databaseLayout.storeFiles();
        // count store files will be checked separately since presence of both files is not required
        storeFiles.remove( databaseLayout.countStoreA() );
        storeFiles.remove( databaseLayout.countStoreB() );
        // index statistics is not mandatory store to have
        storeFiles.remove( databaseLayout.indexStatisticsStore() );
        return storeFiles.stream().allMatch( fileSystem::fileExists ) && oneOfCountStoreFilesExist( databaseLayout, fileSystem );
    }

    private static boolean oneOfCountStoreFilesExist( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        return fileSystem.fileExists( databaseLayout.countStoreA() ) || fileSystem.fileExists( databaseLayout.countStoreB() );
    }
}
