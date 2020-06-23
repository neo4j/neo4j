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
package org.neo4j.kernel.database;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;

final class DatabaseFileHelper
{
    private DatabaseFileHelper()
    {
    }

    static List<Path> filesToKeepOnTruncation( DatabaseLayout databaseLayout )
    {
        DatabaseFile[] filesToKeep = {DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE, DatabaseFile.PROPERTY_KEY_TOKEN_STORE, DatabaseFile.LABEL_TOKEN_NAMES_STORE,
                DatabaseFile.LABEL_TOKEN_STORE, DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE, DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                DatabaseFile.SCHEMA_STORE};
        return Arrays.stream( filesToKeep ).flatMap( databaseLayout::allFiles ).collect( Collectors.toList() );
    }

    static List<Path> filesToDeleteOnTruncation( List<Path> filesToKeep, DatabaseLayout databaseLayout, Path[] transactionLogs )
    {
        List<Path> filesToDelete = new ArrayList<>();
        Collections.addAll( filesToDelete, databaseLayout.listDatabaseFiles( file -> !filesToKeep.contains( file ) ) );
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        if ( !transactionLogsDirectory.equals( databaseLayout.databaseDirectory() ) )
        {
            if ( transactionLogs != null )
            {
                Collections.addAll( filesToDelete, transactionLogs );
            }
        }
        return filesToDelete;
    }
}
