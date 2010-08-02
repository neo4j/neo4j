package slavetest;

import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.SlaveContext;

public class FakeBroker implements Broker
{
    private final Master master;
    private final SlaveContext slaveContext;

    FakeBroker( Master master )
    {
        this.master = master;
        this.slaveContext = new SlaveContext( 1, 0 ); 
    }
    
    public Master getMaster()
    {
        return master;
    }

    public SlaveContext getSlaveContext()
    {
        return slaveContext;
    }
}
