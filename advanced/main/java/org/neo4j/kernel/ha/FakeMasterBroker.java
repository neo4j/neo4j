package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.SlaveContext;

public class FakeMasterBroker extends AbstractBroker
{
    private Master realMaster;
    private MasterServer masterServer;
    
    @Override
    public void setDb( GraphDatabaseService db )
    {
        super.setDb( db );
        this.realMaster = new MasterImpl( db );
        this.masterServer = new MasterServer( realMaster );
    }
    
    public Master getMaster()
    {
        return this.realMaster;
    }

    public SlaveContext getSlaveContext()
    {
        throw new UnsupportedOperationException( "Shouldn't get called on master" );
    }

    @Override
    public boolean thisIsMaster()
    {
        return true;
    }
}
