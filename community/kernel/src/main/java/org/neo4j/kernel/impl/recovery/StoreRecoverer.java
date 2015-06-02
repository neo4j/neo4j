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
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReaderFactory;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;

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
        long logVersion = fs.fileExists( new File( dataDir, NeoStore.DEFAULT_NAME ) )
                          ? new NeoStoreUtil( dataDir, fs ).getLogVersion()
                          : 0;
        return recoveryNeededAt( dataDir, logVersion );
    }

    public boolean recoveryNeededAt( File dataDir, long currentLogVersion ) throws IOException
    {
        // We need config to determine where the logical log files are
        File neoStorePath = new File( dataDir, NeoStore.DEFAULT_NAME );
        if ( !fs.fileExists( neoStorePath ) )
        {
            // No database in the specified directory.
            return false;
        }

        PhysicalLogFiles logFiles = new PhysicalLogFiles( dataDir, fs );

        LogEntryReader<ReadableVersionableLogChannel> reader = new LogEntryReaderFactory().versionable();

        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LatestCheckPointFinder.LatestCheckPoint result = finder.find( currentLogVersion );
        if ( result.checkPoint == null )
        {
            if ( result.oldestLogVersionFound == 0 )
            {
                return true;
            }
            else
            {
                throw new UnderlyingStorageException( "No check point found in any log file from version " +
                                                      Math.max( INITIAL_LOG_VERSION, result.oldestLogVersionFound ) +
                                                      " to " + currentLogVersion );
            }
        }

        return result.commitsAfterCheckPoint;
    }

    public void recover( File dataDir, Map<String,String> params, LogProvider userLogProvider )
    {
        // For now, just launch a full embedded database on top of the
        // directory.
        // In a perfect world, to be expanded to only do recovery, and to be
        // used as a component of the database, rather than something that is bolted
        // on outside it like this.

        new EmbeddedGraphDatabase(
                dataDir.getAbsolutePath(),
                params,
                GraphDatabaseDependencies.newDependencies().userLogProvider( userLogProvider ) )
                .shutdown();
    }
}
