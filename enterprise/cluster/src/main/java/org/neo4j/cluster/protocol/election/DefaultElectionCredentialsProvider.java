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
package org.neo4j.cluster.protocol.election;

import org.neo4j.kernel.impl.core.LastTxIdGetter;

/**
 * ElectionCredentialsProvider that provides the server URI and latest tx id as credentials for elections
 */
public class DefaultElectionCredentialsProvider
    implements ElectionCredentialsProvider
{
    private final int serverId;
    private final LastTxIdGetter lastTxIdGetter;

    public DefaultElectionCredentialsProvider( int serverId, LastTxIdGetter lastTxIdGetter )
    {
        this.serverId = serverId;
        this.lastTxIdGetter = lastTxIdGetter;
    }

    @Override
    public DefaultElectionCredentials getCredentials( String role )
    {
        return new DefaultElectionCredentials( serverId, lastTxIdGetter.getLastTxId() );
    }
}
