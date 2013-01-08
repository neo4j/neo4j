/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class FakeClusterClient implements ClusterClient
{
    private final Broker delegate;

    public FakeClusterClient( Broker delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public int getBackupPort( int machineId )
    {
        throw new UnsupportedOperationException(
                "FakeClusterClient does not provide this info" );
    }

    @Override
    public Machine[] getConnectedSlaves()
    {
        throw new UnsupportedOperationException(
                "FakeClusterClient does not provide this info" );
    }

    @Override
    public Machine getMaster()
    {
        return delegate.getMaster().other();
    }

    @Override
    public Pair<Master, Machine> getMasterClient()
    {
        return delegate.getMaster();
    }
}
