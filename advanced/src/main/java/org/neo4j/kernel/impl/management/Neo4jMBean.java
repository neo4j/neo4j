package org.neo4j.kernel.impl.management;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public class Neo4jMBean extends StandardMBean
{
    private static final Logger log = Logger.getLogger( Neo4jMBean.class.getName() );

    public static Runnable initMBeans( Creator creator )
    {
        Factory jmx = new Factory( getPlatformMBeanServer(), creator );
        creator.create( jmx );
        jmx.createKernelMBean( creator.kernelVersion );
        return new JmxShutdown( jmx.beans );
    }

    public static <T> T getBean( int instanceId, Class<T> beanType )
    {
        if ( beanType.isInterface()
             && beanType.getPackage().getName().equals(
                     "org.neo4j.kernel.management" ) )
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
        throw new IllegalArgumentException( "Not a Neo4j management bean: " + beanType );
    }

    public static abstract class Creator
    {
        private final int id;
        private final String kernelVersion;
        private final NeoStoreXaDataSource datasource;

        protected Creator( int instanceId, String kernelVersion, NeoStoreXaDataSource datasource )
        {
            if ( kernelVersion == null || datasource == null )
            {
                throw new IllegalArgumentException( "null valued argument" );
            }
            this.id = instanceId;
            this.kernelVersion = kernelVersion;
            this.datasource = datasource;
        }

        protected abstract void create( Neo4jMBean.Factory jmx );
    }

    public static final class Factory
    {
        private final MBeanServer mbs;
        private final Creator instance;
        private final List<Neo4jMBean> beans = new ArrayList<Neo4jMBean>();

        private Factory( MBeanServer mbs, Creator creator )
        {
            this.mbs = mbs;
            this.instance = creator;
        }

        private void createKernelMBean( final String kernelVersion )
        {
            if ( !register( new Callable<KernelBean>()
            {
                public KernelBean call() throws Exception
                {
                    return new KernelBean( instance.id, kernelVersion, instance.datasource );
                }
            } ) ) failedToRegister( "KernelBean" );
        }

        public void createPrimitiveMBean( final NodeManager nodeManager )
        {
            if ( !register( new Callable<PrimitivesBean>()
            {
                public PrimitivesBean call() throws NotCompliantMBeanException
                {
                    return new PrimitivesBean( instance.id, nodeManager );
                }
            } ) ) failedToRegister( "PrimitiveMBean" );
        }

        public void createCacheMBean( final NodeManager nodeManager )
        {
            if ( !register( new Callable<CacheBean>()
            {
                public CacheBean call() throws NotCompliantMBeanException
                {
                    return new CacheBean( instance.id, nodeManager );
                }
            } ) ) failedToRegister( "CacheMBean" );
        }

        public void createDynamicConfigurationMBean( final Map<Object, Object> params )
        {
            if ( !register( new Callable<Neo4jMBean>()
            {
                public Neo4jMBean call() throws NotCompliantMBeanException
                {
                    return new Configuration( instance.id, params );
                }
            } ) ) failedToRegister( "ConfigurationMBean" );
        }

        public void createMemoryMappingMBean( XaDataSourceManager datasourceMananger )
        {
            final NeoStoreXaDataSource datasource = (NeoStoreXaDataSource) datasourceMananger.getXaDataSource( "nioneodb" );
            if ( !register( new Callable<Neo4jMBean>()
            {
                public Neo4jMBean call()
                {
                    return MemoryMappingBean.create( instance.id, datasource );
                }
            } ) ) failedToRegister( "MemoryMappingMBean" );
        }

        public void createXaManagerMBean( final XaDataSourceManager datasourceMananger )
        {
            if ( !register( new Callable<Neo4jMBean>()
            {
                public Neo4jMBean call()
                {
                    return XaManagerBean.create( instance.id, datasourceMananger );
                }
            } ) ) failedToRegister( "XaManagerMBean" );
        }

        public void createTransactionManagerMBean( final TxModule txModule )
        {
            if ( !register( new Callable<TransactionManagerBean>()
            {
                public TransactionManagerBean call() throws NotCompliantMBeanException
                {
                    return new TransactionManagerBean( instance.id, txModule );
                }
            } ) ) failedToRegister( "TransactionManagerMBean" );
        }

        public void createLockManagerMBean( final LockManager lockManager )
        {
            if ( !register( new Callable<LockManagerBean>()
            {
                public LockManagerBean call() throws NotCompliantMBeanException
                {
                    return new LockManagerBean( instance.id, lockManager );
                }
            } ) ) failedToRegister( "LockManagerMBean" );
        }

        public void createStoreFileMBean()
        {
            File path;
            try
            {
                path = new File( instance.datasource.getStoreDir() ).getCanonicalFile().getAbsoluteFile();
            }
            catch ( IOException e )
            {
                path = new File( instance.datasource.getStoreDir() ).getAbsoluteFile();
            }
            final File storePath = path;
            if ( !register( new Callable<StoreFileBean>()
            {
                public StoreFileBean call() throws NotCompliantMBeanException
                {
                    return new StoreFileBean( instance.id, storePath );
                }
            } ) ) failedToRegister( "StoreFileMBean" );
        }

        private boolean register( Callable<? extends Neo4jMBean> beanFactory )
        {
            Neo4jMBean bean;
            try
            {
                bean = beanFactory.call();
            }
            catch ( Exception e )
            {
                return false;
            }
            bean = registerBean( mbs, bean );
            if ( bean != null )
            {
                beans.add( bean );
                return true;
            }
            return false;
        }
    }

    private static Neo4jMBean registerBean( MBeanServer mbs, Neo4jMBean bean )
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

    private static void failedToRegister( String mBean )
    {
        log.info( "Failed to register " + mBean );
    }

    private static final class JmxShutdown implements Runnable
    {
        private final Neo4jMBean[] beans;

        public JmxShutdown( List<Neo4jMBean> beans )
        {
            this.beans = beans.toArray( new Neo4jMBean[beans.size()] );
        }

        public void run()
        {
            MBeanServer mbs = getPlatformMBeanServer();
            for ( Neo4jMBean bean : beans )
            {
                unregisterBean( mbs, bean );
            }
        }
    }

    private static void unregisterBean( MBeanServer mbs, Neo4jMBean bean )
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

    private static final ProxyMaker PROXY_MAKER;

    private static class ProxyMaker
    {
        private final Method isMXBeanInterface;
        private final Method newMBeanProxy;
        private final Method newMXBeanProxy;

        ProxyMaker() throws Exception
        {
            Class<?> JMX = Class.forName( "javax.management.JMX" );
            this.isMXBeanInterface = JMX.getMethod( "isMXBeanInterface", Class.class );
            this.newMBeanProxy = JMX.getMethod( "newMBeanProxy", MBeanServerConnection.class,
                    ObjectName.class, Class.class );
            this.newMXBeanProxy = JMX.getMethod( "newMXBeanProxy", MBeanServerConnection.class,
                    ObjectName.class, Class.class );
        }

        <T> T makeProxy( ObjectName name, Class<T> beanType )
        {
            try
            {
                final Method factoryMethod;
                if ( isMXBeanInterface( beanType ) )
                {
                    factoryMethod = newMXBeanProxy;
                }
                else
                {
                    factoryMethod = newMBeanProxy;
                }
                return beanType.cast( factoryMethod.invoke( null, getPlatformMBeanServer(), name,
                        beanType ) );
            }
            catch ( InvocationTargetException exception )
            {
                throw launderRuntimeException( exception.getTargetException() );
            }
            catch ( Exception exception )
            {
                throw new UnsupportedOperationException(
                        "Creating Management Bean proxies requires Java 1.6", exception );
            }
        }

        private boolean isMXBeanInterface( Class<?> interfaceClass ) throws Exception
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

    private static final boolean SUPPORT_MX_BEAN;
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
        SUPPORT_MX_BEAN = proxyMaker != null;
    }

    static abstract class MXFactory<T extends Neo4jMBean>
    {
        abstract T createStandardMBean() throws NotCompliantMBeanException;

        abstract T createMXBean();

        final T createMBean()
        {
            try
            {
                return createStandardMBean();
            }
            catch ( NotCompliantMBeanException cause )
            {
                throw new IllegalArgumentException( cause );
            }
        }
    }

    static <T extends Neo4jMBean> T createMX( MXFactory<T> factory )
    {
        if ( SUPPORT_MX_BEAN )
        {
            return factory.createMXBean();
        }
        else
        {
            return factory.createMBean();
        }
    }

    private final ObjectName objectName;

    Neo4jMBean( int instanceId, Class<?> mbeanIface, boolean isMXBean )
    {
        super( mbeanIface, isMXBean );
        objectName = getObjectName( instanceId, mbeanIface, getClass() );
        if ( objectName == null )
        {
            throw new IllegalArgumentException( "" );
        }
    }

    Neo4jMBean( int instanceId, Class<?> mbeanIface ) throws NotCompliantMBeanException
    {
        super( mbeanIface );
        objectName = getObjectName( instanceId, mbeanIface, getClass() );
        if ( objectName == null )
        {
            throw new IllegalArgumentException( "" );
        }
    }

    Neo4jMBean( int instanceId ) throws NotCompliantMBeanException
    {
        super( DynamicMBean.class );
        objectName = getObjectName( instanceId, DynamicMBean.class, getClass() );
    }

    private static ObjectName getObjectName( int instanceId, Class<?> iface, Class<?> clazz )
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

    @Override
    protected String getDescription( MBeanInfo info )
    {
        Description description = getClass().getAnnotation( Description.class );
        if ( description != null ) return description.value();
        return super.getDescription( info );
    }

    @Override
    protected String getDescription( MBeanAttributeInfo info )
    {
        Description description = describeMethod( info, "get", "is" );
        if ( description != null ) return description.value();
        return super.getDescription( info );
    }

    @Override
    protected String getDescription( MBeanOperationInfo info )
    {
        Description description = describeMethod( info );
        if ( description != null ) return description.value();
        return super.getDescription( info );
    }

    @Override
    protected int getImpact( MBeanOperationInfo info )
    {
        Description description = describeMethod( info );
        if ( description != null ) return description.impact();
        return super.getImpact( info );
    }

    private Description describeMethod( MBeanFeatureInfo info, String... prefixes )
    {
        if ( prefixes == null || prefixes.length == 0 )
        {
            try
            {
                return getClass().getMethod( info.getName() ).getAnnotation( Description.class );
            }
            catch ( Exception e )
            {
                return null;
            }
        }
        else
        {
            for ( String prefix : prefixes )
            {
                try
                {
                    return getClass().getMethod( prefix + info.getName() ).getAnnotation(
                            Description.class );
                }
                catch ( Exception e )
                {
                }
            }
            return null;
        }
    }
}
