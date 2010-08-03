package slavetest;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.SlaveContext;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class FakeBroker implements Broker
{
    private final Master master;
    private GraphDatabaseService slaveDb;

    FakeBroker( Master master )
    {
        this.master = master;
    }
    
    public Master getMaster()
    {
        return master;
    }
    
    void setSlave( GraphDatabaseService db )
    {
        this.slaveDb = db;
    }

    public SlaveContext getSlaveContext()
    {
        Config config = ((HighlyAvailableGraphDatabase) slaveDb).getConfig();
        Map<String, Long> txs = new HashMap<String, Long>();
        for ( XaDataSource dataSource :
                config.getTxModule().getXaDataSourceManager().getAllRegisteredDataSources() )
        {
            txs.put( dataSource.getName(), dataSource.getLastCommittedTxId() );
        }
        return new SlaveContext( 0, txs );
    }
}
