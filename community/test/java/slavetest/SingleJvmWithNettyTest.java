/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package slavetest;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.CommunicationProtocol;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class SingleJvmWithNettyTest extends SingleJvmTest
{
    @Test
    public void assertThatNettyIsUsed() throws Exception
    {
        initializeDbs( 1 );
        assertTrue(
                "Slave Broker is not a client",
                ( (HighlyAvailableGraphDatabase) getSlave( 0 ) ).getBroker().getMaster().first() instanceof MasterClient );
    }

    @Override
    protected Broker makeSlaveBroker( MasterImpl master, int masterId, int id, String storeDir )
    {
        final Machine masterMachine = new Machine( masterId, -1, 1,//
                "localhost:" + CommunicationProtocol.PORT );
        final Master client = new MasterClient( masterMachine, storeDir );
        return new AbstractBroker( id, storeDir )
        {
            public boolean iAmMaster()
            {
                return false;
            }

            public Pair<Master, Machine> getMasterReally()
            {
                return getMaster();
            }

            public Pair<Master, Machine> getMaster()
            {
                return Pair.of( client, masterMachine );
            }

            public Object instantiateMasterServer( GraphDatabaseService graphDb )
            {
                throw new UnsupportedOperationException(
                        "cannot instantiate master server on slave" );
            }
        };
    }
}
