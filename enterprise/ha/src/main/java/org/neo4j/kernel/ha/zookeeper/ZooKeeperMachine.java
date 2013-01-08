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
package org.neo4j.kernel.ha.zookeeper;

import org.neo4j.com.RequestContext;

public class ZooKeeperMachine extends Machine
{
    public static final ZooKeeperMachine NO_MACHINE = new ZooKeeperMachine( -1,
            -1, 1, RequestContext.EMPTY.machineId(), null, -1, "" );

    private final String zkPath;

    public ZooKeeperMachine( int machineId, int sequenceId,
            long lastCommittedTxId, int masterForCommittedTxId, String server,
            int backupPort, String zkPath )
    {
        super( machineId, sequenceId, lastCommittedTxId,
                masterForCommittedTxId, server, backupPort );
        this.zkPath = zkPath;
    }

    public String getZooKeeperPath()
    {
        return zkPath;
    }
}
