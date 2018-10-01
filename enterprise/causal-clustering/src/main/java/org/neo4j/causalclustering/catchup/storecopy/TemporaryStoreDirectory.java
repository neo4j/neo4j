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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;

public class TemporaryStoreDirectory implements AutoCloseable
{
    private static final String TEMP_COPY_DIRECTORY_NAME = "temp-copy";

    private final File tempStoreDir;
    private final DatabaseLayout tempDatabaseLayout;
    private final StoreFiles storeFiles;
    private LogFiles tempLogFiles;
    private boolean keepStore;

    public TemporaryStoreDirectory( FileSystemAbstraction fs, PageCache pageCache, File parent ) throws IOException
    {
        this.tempStoreDir = new File( parent, TEMP_COPY_DIRECTORY_NAME );
        this.tempDatabaseLayout = DatabaseLayout.of( tempStoreDir, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        storeFiles = new StoreFiles( fs, pageCache, ( directory, name ) -> true );
        tempLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( tempDatabaseLayout.databaseDirectory(), fs ).build();
        storeFiles.delete( tempStoreDir, tempLogFiles );
    }

    public File storeDir()
    {
        return tempStoreDir;
    }

    public DatabaseLayout databaseLayout()
    {
        return tempDatabaseLayout;
    }

    void keepStore()
    {
        this.keepStore = true;
    }

    @Override
    public void close() throws IOException
    {
        if ( !keepStore )
        {
            storeFiles.delete( tempStoreDir, tempLogFiles );
        }
    }
}
