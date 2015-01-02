/**
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogFiles;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogRecoveryCheck;

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

    public boolean recoveryNeededAt( File dataDir, Map<String, String> params ) throws IOException
    {
        // We need config to determine where the logical log files are
        params.put( GraphDatabaseSettings.store_dir.name(), dataDir.getPath() );
        Config config = new Config( params, GraphDatabaseSettings.class );

        File baseLogPath = config.get( GraphDatabaseSettings.logical_log );
        XaLogicalLogFiles logFiles = new XaLogicalLogFiles( baseLogPath, fs );

        File log;
        switch ( logFiles.determineState() )
        {
        case CLEAN:
            return false;

        case NO_ACTIVE_FILE:
        case DUAL_LOGS_LOG_1_ACTIVE:
        case DUAL_LOGS_LOG_2_ACTIVE:
            return true;

        case LEGACY_WITHOUT_LOG_ROTATION:
            log = baseLogPath;
            break;

        case LOG_1_ACTIVE:
            log = logFiles.getLog1FileName();
            break;

        case LOG_2_ACTIVE:
            log = logFiles.getLog2FileName();
            break;

        default:
            return true;
        }

        StoreChannel logChannel = null;
        try
        {
            logChannel = fs.open( log, "r" );
            return new XaLogicalLogRecoveryCheck( logChannel ).recoveryRequired();
        }
        finally
        {
            if ( logChannel != null )
            {
                logChannel.close();
            }
        }
    }

    public void recover( File dataDir, Map<String, String> params ) throws IOException
    {
        // For now, just launch a full embedded database on top of the
        // directory.
        // In a perfect world, to be expanded to only do recovery, and to be
        // used
        // as a component of the database, rather than something that is bolted
        // on outside it like this.

        GraphDatabaseService db =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dataDir.getCanonicalPath() )
                    .setConfig( params ).newGraphDatabase();

        db.shutdown();
    }
}
