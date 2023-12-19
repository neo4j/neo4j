/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;

class BackupTransactionLogFilesHelper
{
    LogFiles readLogFiles( File backupDir ) throws IOException
    {
        FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction );
        return LogFilesBuilder.activeFilesBuilder( backupDir, fileSystemAbstraction, pageCache ).build();
    }
}
