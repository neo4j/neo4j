package org.neo4j.kernel.management;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public abstract class Neo4jJmx
{
    private static final Logger log = Logger.getLogger( Neo4jJmx.class.getName() );

    public static Runnable initJMX( Neo4jJmx.Creator creator )
    {
        Factory jmx = new Factory( getPlatformMBeanServer(), creator );
        creator.create( jmx );
        jmx.createKernelMBean( creator.kernelVersion );
        return new JmxShutdown( jmx.beans );
    }

    public static <T> T getBean( int instanceId, Class<T> beanType )
    {
        if ( beanType.isInterface() && beanType.getName().endsWith( "Bean" )
             && beanType.getPackage().equals( Neo4jJmx.class.getPackage() ) )
        {
            if ( PROXY_MAKER == null )
            {
                throw new UnsupportedOperationException(
                        "Creating Management Bean proxies requires Java 1.6" );
            }
            else
            {
                ObjectName name = getObjectName( instanceId, beanType, null );
                return PROXY_MAKER.makeProxy( name, beanType );
            }
        }
        throw new IllegalArgumentException( "Not a Neo4j management bean: "
                                            + beanType );
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
        private final Creator instance;
        private final List<Neo4jJmx> beans = new ArrayList<Neo4jJmx>();

        private Factory( MBeanServer mbs, Creator creator )
        {
            this.mbs = mbs;
            this.instance = creator;
        }

        private void createKernelMBean( String kernelVersion )
        {
            if ( !register( new Kernel( instance.id, kernelVersion,
                    instance.datasource ) ) )
                failedToRegister( "KernelMBean" );
        }

        public void createPrimitiveMBean( NodeManager nodeManager )
        {
            if ( !register( new Primitive( instance.id, nodeManager ) ) )
                failedToRegister( "PrimitiveMBean" );
        }

        public void createCacheMBean( NodeManager nodeManager )
        {
            if ( !register( new Cache( instance.id, nodeManager ) ) )
                failedToRegister( "CacheMBean" );
        }

        public void createDynamicConfigurationMBean( Map<Object, Object> params )
        {
            if ( !register( new Configuration( instance.id, params ) ) )
                failedToRegister( "ConfigurationMBean" );
        }

        public void createMemoryMappingMBean(
                XaDataSourceManager datasourceMananger )
        {
            NeoStoreXaDataSource datasource = (NeoStoreXaDataSource) datasourceMananger.getXaDataSource( "nioneodb" );
            if ( !register( new MemoryMapping.AsMxBean( instance.id, datasource ) ) )
            {
                if ( !register( new MemoryMapping( instance.id, datasource ) ) )
                    failedToRegister( "MemoryMappingMBean" );
            }
        }

        public void createXaManagerMBean( XaDataSourceManager datasourceMananger )
        {
            if ( !register( new XaManager.AsMXBean( instance.id, datasourceMananger ) ) )
            {
                if ( !register( new XaManager( instance.id, datasourceMananger ) ) )
                    failedToRegister( "XaManagerMBean" );
            }
        }

        public void createTransactionManagerMBean( TxModule txModule )
        {
            if ( !register( new TransactionManager( instance.id, txModule ) ) )
                failedToRegister( "TransactionManagerMBean" );
        }

        public void createLockManagerMBean(
                org.neo4j.kernel.impl.transaction.LockManager lockManager )
        {
            if ( !register( new LockManager( instance.id, lockManager ) ) )
                failedToRegister( "LockManagerMBean" );
        }

        public void createStoreFileMBean()
        {
            File storePath;
            try
            {
                storePath = new File( instance.datasource.getStoreDir() ).getCanonicalFile().getAbsoluteFile();
            }
            catch ( IOException e )
            {
                storePath = new File( instance.datasource.getStoreDir() ).getAbsoluteFile();
            }
            if ( !register( new StoreFile( instance.id, storePath ) ) )
                failedToRegister( "StoreFileMBean" );
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

    private static final ProxyMaker PROXY_MAKER;

    private static class ProxyMaker
    {
        private final Method isMXBeanInterface;
        private final Method newMBeanProxy;
        private final Method newMXBeanProxy;

        ProxyMaker() throws Exception
        {
            Class<?> JMX = Class.forName( "javax.management.JMX" );
            this.isMXBeanInterface = JMX.getMethod( "isMXBeanInterface",
                    Class.class );
            this.newMBeanProxy = JMX.getMethod( "newMBeanProxy",
                    MBeanServerConnection.class, ObjectName.class, Class.class );
            this.newMXBeanProxy = JMX.getMethod( "newMXBeanProxy",
                    MBeanServerConnection.class, ObjectName.class, Class.class );
        }

        <T> T makeProxy( ObjectName name, Class<T> beanType )
        {
            try {
                final Method factoryMethod;
                if ( isMXBeanInterface( beanType ) )
                {
                    factoryMethod = newMXBeanProxy;
                }
                else
                {
                    factoryMethod = newMBeanProxy;
                }
                return beanType.cast( factoryMethod.invoke( null,
                        getPlatformMBeanServer(), name, beanType ) );
            }
            catch ( InvocationTargetException exception )
            {
                throw launderRuntimeException( exception.getTargetException() );
            }
            catch ( Exception exception )
            {
                throw new UnsupportedOperationException(
                        "Creating Management Bean proxies requires Java 1.6",
                        exception );
            }
        }

        private boolean isMXBeanInterface( Class<?> interfaceClass )
                throws Exception
        {
            return (Boolean) isMXBeanInterface.invoke( null, interfaceClass );
        }

        static RuntimeException launderRuntimeException( Throwable exception )
        {
            if ( exception instanceof RuntimeException )
            {
                return (RuntimeException) exception;
            }
            else if ( exception instanceof Error )
            {
                throw (Error) exception;
            }
            else
            {
                throw new RuntimeException( "Unexpected Exception!", exception );
            }
        }
    }

    static
    {
        ProxyMaker proxyMaker;
        try
        {
            proxyMaker = new ProxyMaker();
        }
        catch ( Throwable t )
        {
            proxyMaker = null;
        }
        PROXY_MAKER = proxyMaker;
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
            log.warning( "Failed to unregister JMX Bean " + bean );
            e.printStackTrace();
        }
    }

    private static void failedToRegister( String mBean )
    {
        log.info( "Failed to register " + mBean );
    }

    private final ObjectName objectName;

    Neo4jJmx( int instanceId )
    {
        ObjectName name = null;
        for ( Class<?> beanType : getClass().getInterfaces() )
        {
            name = getObjectName( instanceId, beanType, getClass() );
        }
        if ( name == null )
        {
            throw new IllegalArgumentException( "" );
        }
        objectName = name;
    }

    private static ObjectName getObjectName( int instanceId, Class<?> iface,
            Class<?> clazz )
    {
        final String name;
        if ( iface == DynamicMBean.class )
        {
            name = clazz.getSimpleName();
        }
        else
        {
            try
            {
                name = (String) iface.getField( "NAME" ).get( null );
            }
            catch ( Exception e )
            {
                return null;
            }
        }
        StringBuilder identifier = new StringBuilder( "org.neo4j:" );
        identifier.append( "instance=kernel#" );
        identifier.append( instanceId );
        identifier.append( ",name=" );
        identifier.append( name );
        try
        {
            return new ObjectName( identifier.toString() );
        }
        catch ( MalformedObjectNameException e )
        {
            return null;
        }
    }
}
