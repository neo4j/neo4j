package org.neo4j.kernel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.NotCompliantMBeanException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.SlaveContext;
import org.neo4j.kernel.impl.management.Description;
import org.neo4j.kernel.impl.management.Neo4jMBean;
import org.neo4j.kernel.management.HighAvailability;
import org.neo4j.kernel.management.SlaveInfo;
import org.neo4j.kernel.management.SlaveInfo.SlaveTransaction;

@Description( "Information about an instance participating in a HA cluster" )
public final class HighAvailabilityBean extends Neo4jMBean implements HighAvailability
{
    private final HighlyAvailableGraphDatabase db;
    
    public HighAvailabilityBean( String instanceId, GraphDatabaseService db )
            throws NotCompliantMBeanException
    {
        super( instanceId, HighAvailability.class );
        this.db = (HighlyAvailableGraphDatabase) db;
    }

    @Description( "The identifier used to identify this machine in the HA cluster" )
    public String getMachineId()
    {
        return Integer.toString( db.getMachineId() );
    }

    @Description( "Whether this instance is master or not" )
    public boolean isMaster()
    {
        return db.getMasterServerIfMaster() != null;
    }

    @Description( "(If this is a master) Information about "
                  + "the instances connected to this instance" )
    public SlaveInfo[] getConnectedSlaves()
    {
        MasterServer master = db.getMasterServerIfMaster();
        if ( master == null ) return null;
        List<SlaveInfo> result = new ArrayList<SlaveInfo>();
        for ( Map.Entry<Integer, Collection<SlaveContext>> entry : master.getSlaveInformation().entrySet() )
        {
            result.add( slaveInfo( entry.getKey(), entry.getValue() ) );
        }
        return result.toArray( new SlaveInfo[result.size()] );
    }

    @Description( "(If this is a slave) Update the database on this "
                  + "instance with the latest transactions from the master" )
    public String update()
    {
        long time = System.currentTimeMillis();
        try
        {
            db.pullUpdates();
        }
        catch ( Exception e )
        {
            return "Update failed: " + e;
        }
        time = System.currentTimeMillis() - time;
        return "Update completed in " + time + "ms";
    }

    private static SlaveInfo slaveInfo( Integer machineId, Collection<SlaveContext> contexts )
    {
        List<SlaveTransaction> txInfo = new ArrayList<SlaveTransaction>();
        for ( SlaveContext context : contexts )
        {
            Map<String, Long> lastTransactions = new HashMap<String, Long>();
            for ( Pair<String, Long> tx : context.lastAppliedTransactions() )
            {
                lastTransactions.put( tx.first(), tx.other() );
            }
            txInfo.add( new SlaveTransaction( context.getEventIdentifier(), lastTransactions ) );
        }
        return new SlaveInfo( machineId, txInfo.toArray( new SlaveTransaction[txInfo.size()] ) );
    }
}
