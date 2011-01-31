/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.ha.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public abstract class RootPathGetter
{
    public abstract Pair<String, Long> getRootPath( ZooKeeper keeper );
    
    public static RootPathGetter forKnownStore( final String storeDir )
    {
        return new RootPathGetter()
        {
            @Override
            public Pair<String, Long> getRootPath( ZooKeeper keeper )
            {
                NeoStoreUtil store = new NeoStoreUtil( storeDir );
                return Pair.of( "/" + store.getCreationTime() + "_" + store.getStoreId(), store.getLastCommittedTx() );
            }
        };
    }
    
    public static RootPathGetter forUnknownStore( final String storeDir )
    {
        return new RootPathGetter()
        {
            @Override
            public Pair<String, Long> getRootPath( ZooKeeper keeper )
            {
                String result = ClusterManager.getSingleRootPath( keeper );
                if ( result == null )
                {
                    new EmbeddedGraphDatabase( storeDir ).shutdown();
                    return RootPathGetter.forKnownStore( storeDir ).getRootPath( keeper );
                }
                else
                {
                    return Pair.of( result, 1L );
                }
            }
        };
    }
}
