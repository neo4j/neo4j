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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    static StoreFilesInfo checkStoreFiles( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        Set<File> storeFiles = databaseLayout.storeFiles();
        // count store, index statistics and label scan store are not mandatory stores to have since they can be automatically rebuilt
        storeFiles.remove( databaseLayout.countStore() );
        storeFiles.remove( databaseLayout.indexStatisticsStore() );
        storeFiles.remove( databaseLayout.labelScanStore() );
        return collectStoreFilesInfo( fileSystem, storeFiles );
    }

    private static StoreFilesInfo collectStoreFilesInfo( FileSystemAbstraction fileSystem, Set<File> storeFiles )
    {
        List<File> missingFiles = storeFiles.stream().filter( file -> !fileSystem.fileExists( file ) ).collect( Collectors.toList() );
        return new StoreFilesInfo( missingFiles );
    }

    static class StoreFilesInfo
    {
        private final List<File> missingStoreFiles;

        StoreFilesInfo( List<File> missingFiles )
        {
            this.missingStoreFiles = missingFiles;
        }

        List<File> getMissingStoreFiles()
        {
            return missingStoreFiles;
        }

        boolean allFilesPresent()
        {
            return missingStoreFiles.isEmpty();
        }
    }
}
