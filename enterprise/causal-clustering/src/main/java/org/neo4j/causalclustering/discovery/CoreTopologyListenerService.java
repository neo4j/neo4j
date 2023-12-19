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
package org.neo4j.causalclustering.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class CoreTopologyListenerService
{
    private final Set<CoreTopologyService.Listener> listeners;

    CoreTopologyListenerService()
    {
        this.listeners = ConcurrentHashMap.newKeySet();
    }

    void addCoreTopologyListener( CoreTopologyService.Listener listener )
    {
        listeners.add( listener );
    }

    void removeCoreTopologyListener( CoreTopologyService.Listener listener )
    {
        listeners.remove( listener );
    }

    void notifyListeners( CoreTopology coreTopology )
    {
        for ( CoreTopologyService.Listener listener : listeners )
        {
            String dbName = listener.dbName();

            listener.onCoreTopologyChange( coreTopology.filterTopologyByDb( dbName ) );
        }
    }
}
