package org.neo4j.kernel.ha;

import org.neo4j.kernel.impl.ha.Master;

public class FakeSlaveBroker extends AbstractBroker
{
    private final Master master;
    
    public FakeSlaveBroker()
    {
        this.master = new MasterClient();
    }
    
    public Master getMaster()
    {
        return master;
    }
    
    @Override
    public boolean thisIsMaster()
    {
        return false;
    }
}
