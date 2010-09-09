package org.neo4j.kernel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.SlaveContext;
import org.neo4j.kernel.impl.management.ManagementBeanProvider;
import org.neo4j.kernel.impl.management.Neo4jMBean;
import org.neo4j.kernel.management.HighAvailability;
import org.neo4j.kernel.management.SlaveInfo;
import org.neo4j.kernel.management.SlaveInfo.SlaveTransaction;

@Service.Implementation( ManagementBeanProvider.class )
public final class HighAvailabilityBean extends ManagementBeanProvider
{
    public HighAvailabilityBean()
    {
        super( HighAvailability.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new HaManager( this, kernel );
    }

    @Override
    protected Neo4jMBean createMXBean( KernelData kernel )
    {
        return new HaManager( this, kernel, true );
    }

    private static class HaManager extends Neo4jMBean implements HighAvailability
    {
        private final HighlyAvailableGraphDatabase haDb;

        HaManager( ManagementBeanProvider provider, KernelData kernel, boolean isMXBean )
        {
            super( provider, kernel, isMXBean );
            this.haDb = (HighlyAvailableGraphDatabase) kernel.graphDatabase();
        }

        HaManager( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.haDb = (HighlyAvailableGraphDatabase) kernel.graphDatabase();
        }

        public String getMachineId()
        {
            return Integer.toString( haDb.getMachineId() );
        }

        public boolean isMaster()
        {
            return haDb.getMasterServerIfMaster() != null;
        }

        public SlaveInfo[] getConnectedSlaves()
        {
            MasterServer master = haDb.getMasterServerIfMaster();
            if ( master == null ) return null;
            List<SlaveInfo> result = new ArrayList<SlaveInfo>();
            for ( Map.Entry<Integer, Collection<SlaveContext>> entry : master.getSlaveInformation().entrySet() )
            {
                result.add( slaveInfo( entry.getKey(), entry.getValue() ) );
            }
            return result.toArray( new SlaveInfo[result.size()] );
        }

        public String update()
        {
            long time = System.currentTimeMillis();
            try
            {
                haDb.pullUpdates();
            }
            catch ( Exception e )
            {
                return "Update failed: " + e;
            }
            time = System.currentTimeMillis() - time;
            return "Update completed in " + time + "ms";
        }
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
