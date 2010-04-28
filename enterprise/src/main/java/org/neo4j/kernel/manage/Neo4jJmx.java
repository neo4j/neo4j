package org.neo4j.kernel.manage;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public abstract class Neo4jJmx
{
    /*
    public static <BEAN> Iterable<BEAN> getBeans( Class<BEAN> beanType )
    {
        if ( !beanType.getPackage().equals( Neo4jJmx.class.getPackage() ) )
        {
            throw new IllegalArgumentException( "Not a Neo4j JMX Bean." );
        }
        throw new UnsupportedOperationException( "not implemented" );
    }
    */
    public static Runnable initJMX( Neo4jJmx.Creator creator )
    {
        Factory jmx = new Factory( getPlatformMBeanServer(), creator.id );
        creator.create( jmx );
        jmx.createKernelMBean( creator.kernelVersion, creator.datasource );
        return new JmxShutdown( jmx.beans );
    }

    public static abstract class Creator
    {
        private final int id;
        private final String kernelVersion;
        private final NeoStoreXaDataSource datasource;

        protected Creator( int instanceId, String kernelVersion,
                NeoStoreXaDataSource datasource )
        {
            if ( kernelVersion == null || datasource == null )
            {
                throw new IllegalArgumentException( "null valued argument" );
            }
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
        private final List<Neo4jJmx> beans = new ArrayList<Neo4jJmx>();

        private Factory( MBeanServer mbs, int instanceId )
        {
            this.mbs = mbs;
            this.instanceId = instanceId;
        }

        private void createKernelMBean( String kernelVersion,
                NeoStoreXaDataSource datasource )
        {
            if ( !register( new Kernel( instanceId, kernelVersion, datasource ) ) )
                failedToRegister( "KernelMBean" );
        }

        public void createPrimitiveMBean( NodeManager nodeManager )
        {
            if ( !register( new Primitive( instanceId, nodeManager ) ) )
                failedToRegister( "KernelMBean" );
        }

        public void createCacheMBean( NodeManager nodeManager )
        {
            if ( !register( new Cache( instanceId, nodeManager ) ) )
                failedToRegister( "CacheMBean" );
        }

        public void createDynamicConfigurationMBean( Map<Object, Object> params )
        {
            if ( !register( new Configuration( instanceId, params ) ) )
                failedToRegister( "ConfigurationMBean" );
        }

        public void createMemoryMappingMBean( XaDataSourceManager datasourceMananger )
        {
            NeoStoreXaDataSource datasource = (NeoStoreXaDataSource)
                    datasourceMananger.getXaDataSource( "nioneodb" );
            if ( !register( new MemoryMappingMonitor.MXBeanImplementation(
                    instanceId, datasource ) ) )
            {
                if ( !register( new MemoryMappingMonitor.MemoryMapping(
                        instanceId, datasource ) ) )
                    failedToRegister( "MemoryMappingMBean" );
            }
        }

        public void createXaManagerMBean( XaDataSourceManager datasourceMananger )
        {
            if ( !register( new XaMonitor.MXBeanImplementation( instanceId,
                    datasourceMananger ) ) )
            {
                if ( !register( new XaMonitor.XaManager( instanceId,
                        datasourceMananger ) ) )
                    failedToRegister( "XaManagerMBean" );
            }
        }

        public void createTransactionManagerMBean( TxModule txModule )
        {
            if ( !register( new TransactionManager( instanceId, txModule ) ) )
                failedToRegister( "TransactionManagerMBean" );
        }

        public void createLockManagerMBean()
        {
            if ( !register( new LockManager( instanceId ) ) )
                failedToRegister( "LockManagerMBean" );
        }

        private boolean register( Neo4jJmx bean )
        {
            bean = registerBean( mbs, bean );
            if ( bean != null )
            {
                beans.add( bean );
                return true;
            }
            return false;
        }
    }

    private static Neo4jJmx registerBean( MBeanServer mbs, Neo4jJmx bean )
    {
        try
        {
            mbs.registerMBean( bean, bean.objectName );
            return bean;
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    private static final class JmxShutdown implements Runnable
    {
        private final Neo4jJmx[] beans;

        public JmxShutdown( List<Neo4jJmx> beans )
        {
            this.beans = beans.toArray( new Neo4jJmx[beans.size()] );
        }

        public void run()
        {
            MBeanServer mbs = getPlatformMBeanServer();
            for ( Neo4jJmx bean : beans )
            {
                unregisterBean( mbs, bean );
            }
        }
    }

    private static void unregisterBean( MBeanServer mbs, Neo4jJmx bean )
    {
        try
        {
            mbs.unregisterMBean( bean.objectName );
        }
        catch ( Exception e )
        {
            System.err.println( "Failed to unregister JMX Bean " + bean );
            e.printStackTrace();
        }
    }

    private static void failedToRegister( String mBean )
    {
        System.err.println( "Failed to register " + mBean );
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
