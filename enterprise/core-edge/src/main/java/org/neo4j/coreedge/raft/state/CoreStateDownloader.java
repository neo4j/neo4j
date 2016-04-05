/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.state;

import java.io.IOException;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.catchup.storecopy.edge.state.StateFetcher;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CoreStateDownloader
{
    private final LocalDatabase localDatabase;
    private final StoreFetcher storeFetcher;
    private final StateFetcher stateFetcher;
    private final Log log;

    public CoreStateDownloader( LocalDatabase localDatabase, StoreFetcher storeFetcher, StateFetcher stateFetcher, LogProvider logProvider )
    {
        this.localDatabase = localDatabase;
        this.storeFetcher = storeFetcher;
        this.stateFetcher = stateFetcher;
        this.log = logProvider.getLog( getClass() );
    }

    void downloadSnapshot( AdvertisedSocketAddress source, CoreState receiver ) throws InterruptedException, StoreCopyFailedException
    {
        localDatabase.stop();

        try
        {
            log.info( "Downloading snapshot from core server at %s", source );

            localDatabase.copyStoreFrom( source, storeFetcher );
            stateFetcher.copyRaftState( source, receiver );

            localDatabase.start();
        }
        catch ( StoreCopyFailedException e )
        {
            log.warn( "Failed to download snapshot", e );
        }
        catch ( IOException e )
        {
            localDatabase.panic( e );
        }
    }
}
