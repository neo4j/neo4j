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
package org.neo4j.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.Map;

public final class SlaveInfo extends InstanceInfo
{
    private static final long serialVersionUID = 1L;

    public static final class SlaveTransaction implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private final int eventId;
        private final Map<String, Long> lastTransactions;

        @ConstructorProperties( { "eventIdentifier", "lastTransaction" } )
        public SlaveTransaction( int eventId, Map<String, Long> lastTransactions )
        {
            this.eventId = eventId;
            this.lastTransactions = lastTransactions;
        }

        public int getEventIdentifier()
        {
            return eventId;
        }

        public long getLastTransaction( String resource )
        {
            return lastTransactions.get( resource ).longValue();
        }
    }

    private final SlaveTransaction[] txInfo;

    @ConstructorProperties( { "address", "instanceId", "machineId", "master",
            "lastCommittedTransactionId", "txInfo" } )
    public SlaveInfo( String address, String instanceId, int machineId, boolean master,
            long lastTxId, SlaveTransaction... txInfo )
    {
        super( address, instanceId, machineId, master, lastTxId );
        this.txInfo = txInfo;
    }

    public SlaveTransaction[] getTxInfo()
    {
        return txInfo;
    }
}
