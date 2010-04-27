package org.neo4j.kernel.manage;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.util.Map;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

public abstract class Neo4jJmx
{
    public static void initJMX( Neo4jJmx.Creator creator )
    {
        Factory jmx = new Factory( getPlatformMBeanServer(), creator.id );
        creator.create( jmx );
        jmx.createKernelMBean( creator.kernelVersion, creator.datasource );
    }

    public static abstract class Creator
    {
        private final int id;
        private final String kernelVersion;
        private final NeoStoreXaDataSource datasource;

        protected Creator( int instanceId, String kernelVersion,
                NeoStoreXaDataSource datasource )
        {
            this.id = instanceId;
            this.kernelVersion = kernelVersion;
            this.datasource = datasource;
        }

        protected abstract void create( Neo4jJmx.Factory jmx );
    }

    public static final class Factory
    {
        private final MBeanServer mbs;
        private final int instanceId;

        private Factory( MBeanServer mbs, int instanceId )
        {
            this.mbs = mbs;
            this.instanceId = instanceId;
        }

        private void createKernelMBean( String kernelVersion, NeoStoreXaDataSource datasource )
        {
            register( mbs, new Kernel( instanceId, kernelVersion, datasource ) );
        }

        public void createPrimitiveMBean( NodeManager nodeManager )
        {
            register( mbs, new Primitive( instanceId, nodeManager ) );
        }

        public void createCacheMBean( NodeManager nodeManager )
        {
            register( mbs, new Cache( instanceId, nodeManager ) );
        }

        public void createDynamicConfigurationMBean( Map<Object, Object> params )
        {
            register( mbs, new Configuration( instanceId, params ) );
        }
    }

    private static void register( MBeanServer mbs, Neo4jJmx monitor )
    {
        try
        {
            mbs.registerMBean( monitor, monitor.objectName );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    private final ObjectName objectName;

    Neo4jJmx( int instanceId )
    {
        StringBuilder identifier = new StringBuilder( "org.neo4j:" );
        identifier.append( "instance=kernel#" );
        identifier.append( instanceId );
        identifier.append( ",name=" );
        identifier.append( getName( getClass() ) );
        try
        {
            objectName = new ObjectName( identifier.toString() );
        }
        catch ( MalformedObjectNameException e )
        {
            throw new IllegalArgumentException( e );
        }
    }

    private static Object getName( Class<? extends Neo4jJmx> clazz )
    {
        for ( Class<?> iface : clazz.getInterfaces() )
        {
            if ( iface == DynamicMBean.class )
            {
                return clazz.getSimpleName();
            }
            try
            {
                return iface.getField( "NAME" ).get( null );
            }
            catch ( Exception e )
            {
                // Had no NAME field
            }
        }
        throw new IllegalStateException( "Invalid Neo4jMonitor implementation." );
    }
}
