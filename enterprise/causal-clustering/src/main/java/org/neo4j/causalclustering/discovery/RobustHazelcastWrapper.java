/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import com.hazelcast.core.HazelcastInstance;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A class which attempts to capture behaviours necessary to make interacting
 * with hazelcast robust, e.g. reconnect on failures. This class is not aimed
 * at high-performance and thus uses synchronization heavily.
 */
class RobustHazelcastWrapper
{
    private final HazelcastConnector connector;
    private HazelcastInstance hzInstance;
    private boolean shutdown;

    RobustHazelcastWrapper( HazelcastConnector connector )
    {
        this.connector = connector;
    }

    synchronized void shutdown()
    {
        if ( hzInstance != null )
        {
            hzInstance.shutdown();
            hzInstance = null;
            shutdown = true;
        }
    }

    private synchronized HazelcastInstance tryEnsureConnection() throws HazelcastInstanceNotActiveException
    {
        if ( shutdown )
        {
            throw new HazelcastInstanceNotActiveException( "Shutdown" );
        }

        if ( hzInstance == null )
        {
            hzInstance = connector.connectToHazelcast();
        }
        return hzInstance;
    }

    private synchronized void invalidateConnection()
    {
        hzInstance = null;
    }

    synchronized <T> T apply( Function<HazelcastInstance,T> function ) throws HazelcastInstanceNotActiveException
    {
        HazelcastInstance hzInstance = tryEnsureConnection();

        try
        {
            return function.apply( hzInstance );
        }
        catch ( com.hazelcast.core.HazelcastInstanceNotActiveException e )
        {
            invalidateConnection();
            throw new HazelcastInstanceNotActiveException( e );
        }
    }

    synchronized void perform( Consumer<HazelcastInstance> operation ) throws HazelcastInstanceNotActiveException
    {
        HazelcastInstance hzInstance = tryEnsureConnection();

        try
        {
            operation.accept( hzInstance );
        }
        catch ( com.hazelcast.core.HazelcastInstanceNotActiveException e )
        {
            invalidateConnection();
            throw new HazelcastInstanceNotActiveException( e );
        }
    }
}
