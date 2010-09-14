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

public class SingleJvmWithNettyTesting extends SingleJvmTesting
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
