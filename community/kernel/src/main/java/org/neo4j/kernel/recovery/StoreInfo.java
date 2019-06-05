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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;

import static org.neo4j.kernel.recovery.RecoveryStoreFileHelper.allStoreFilesExist;

class StoreInfo
{
    private final boolean allStoreFilesPresent;
    private final boolean firstLogFileExist;

    StoreInfo( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        allStoreFilesPresent = allStoreFilesExist( databaseLayout, fileSystem );
        firstLogFileExist = isFirstTransactionLogFileExist( databaseLayout, fileSystem );
    }

    boolean isAllStoreFilesPresent()
    {
        return allStoreFilesPresent;
    }

    boolean isFirstLogFileExist()
    {
        return firstLogFileExist;
    }

    private static boolean isFirstTransactionLogFileExist( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        TransactionLogFilesHelper logFilesHelper = new TransactionLogFilesHelper( fileSystem, databaseLayout.getTransactionLogsDirectory() );
        return fileSystem.fileExists( logFilesHelper.getLogFileForVersion( 0 ) );
    }
}
