package slavetest;

import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.SlaveContext;

public class FakeBroker implements Broker
{
    public Master getMaster()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public SlaveContext getSlaveContext()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
