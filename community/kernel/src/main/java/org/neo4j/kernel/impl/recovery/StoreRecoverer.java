/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.recovery;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;
import org.neo4j.kernel.impl.transaction.log.LogRecoveryCheck;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.logging.Logging;

/**
 * For now, an external tool that can determine if a given store will need
 * recovery, and perform recovery on given stores.
 */
public class StoreRecoverer
{
    private final FileSystemAbstraction fs;

    public StoreRecoverer()
    {
        this( new DefaultFileSystemAbstraction() );
    }

    public StoreRecoverer( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public boolean recoveryNeededAt( File dataDir ) throws IOException
    {
        long logVersion = fs.fileExists( new File( dataDir, NeoStore.DEFAULT_NAME ) ) ?
                new NeoStoreUtil( dataDir, fs ).getLogVersion() : 0;
        return recoveryNeededAt( dataDir, logVersion );
    }

    public boolean recoveryNeededAt( File dataDir, long currentLogVersion )
            throws IOException
    {
        // We need config to determine where the logical log files are
        File neoStorePath = new File( dataDir, NeoStore.DEFAULT_NAME );
        if ( !fs.fileExists( neoStorePath ) )
        {
            // No database in the specified directory.
            return false;
        }

        PhysicalLogFiles logFiles = new PhysicalLogFiles( dataDir, fs );
        File log = logFiles.getLogFileForVersion( currentLogVersion );

        if ( !fs.fileExists( log ) )
        {
            // This most likely means that the db has been cleanly shut down, i.e. force then inc log version,
            // then NOT creating a new log file (will be done the next startup)
            return false;
        }
        try ( StoreChannel logChannel = fs.open( log, "r" ) )
        {
            return LogRecoveryCheck.recoveryRequired(logChannel);
        }
    }

    public void recover( File dataDir, Map<String, String> params, Logging logging )
    {
        // For now, just launch a full embedded database on top of the
        // directory.
        // In a perfect world, to be expanded to only do recovery, and to be
        // used as a component of the database, rather than something that is bolted
        // on outside it like this.

        new EmbeddedGraphDatabase(
                dataDir.getAbsolutePath(),
                params,
                GraphDatabaseDependencies.newDependencies().logging(logging ) )
            .shutdown();
    }
}
