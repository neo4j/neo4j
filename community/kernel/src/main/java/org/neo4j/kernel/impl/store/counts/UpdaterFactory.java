/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.impl.api.CountsAccessor.Updater;


class UpdaterFactory
{
    Updater getUpdater( CountsStore countsStore, long txId, TransactionApplicationMode mode )
    {
        if ( mode == TransactionApplicationMode.RECOVERY && countsStore.haveSeenTxId( txId ) )
        {
            return new Updater()
            {
                @Override
                public void incrementNodeCount( int labelId, long delta )
                {
                }

                @Override
                public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
                {
                }

                @Override
                public void close()
                {
                }
            };
        }
        else
        {
            return new Updater()
            {
                private final ConcurrentHashMap<CountsKey,long[]> updates = new ConcurrentHashMap<>();

                @Override
                public void incrementNodeCount( int labelId, long delta )
                {
                    updates.put( CountsKeyFactory.nodeKey( labelId ), new long[]{delta} );
                }

                @Override
                public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
                {
                    updates.put( CountsKeyFactory.relationshipKey( startLabelId, typeId, endLabelId ),
                            new long[]{delta} );
                }

                @Override
                public void close()
                {
                    if ( countsStore != null )
                    {
                        countsStore.updateAll( txId, updates );
                    }
                }
            };
        }
    }
}
