package org.neo4j.management.impl;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Logger;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;

@Service.Implementation( KernelExtension.class )
public final class JmxExtension extends KernelExtension
{
    private static final Logger log = Logger.getLogger( JmxExtension.class.getName() );
    public JmxExtension()
    {
        super( "kernel jmx" );
    }

    @Override
    protected void load( KernelData kernel )
    {
        kernel.setState( this, loadBeans( kernel ) );
    }

    @Override
    protected void unload( KernelData kernel )
    {
        ( (ShutdownHook) kernel.getState( this ) ).shutdown();
    }

    private ShutdownHook loadBeans( KernelData kernel )
    {
        MBeanServer mbs = getPlatformMBeanServer();
        List<Neo4jMBean> beans = new ArrayList<Neo4jMBean>();
        for ( ManagementBeanProvider provider : Service.load( ManagementBeanProvider.class ) )
        {
            try
            {
                Neo4jMBean bean = provider.loadBeen( kernel );
                if ( bean != null )
                {
                    mbs.registerMBean( bean, bean.objectName );
                    beans.add( bean );
                }
            }
            catch ( Exception e )
            {
                log.info( "Failed to register JMX Bean " + provider );
            }
        }
        try
        {
            Neo4jMBean bean = new KernelBean( kernel );
            mbs.registerMBean( bean, bean.objectName );
            beans.add( bean );
        }
        catch ( Exception e )
        {
            log.info( "Failed to register Kernel JMX Bean" );
        }
        return new ShutdownHook( beans.toArray( new Neo4jMBean[beans.size()] ) );
    }

    private static final class ShutdownHook
    {
        private final Neo4jMBean[] beans;

        ShutdownHook( Neo4jMBean[] beans )
        {
            this.beans = beans;
        }

        void shutdown()
        {
            MBeanServer mbs = getPlatformMBeanServer();
            for ( Neo4jMBean bean : beans )
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
        }
    }

    public <T> T getBean( KernelData kernel, Class<T> beanInterface )
    {
        if ( !isLoaded( kernel ) ) throw new IllegalStateException( "Not Loaded!" );
        ObjectName name = getObjectName( kernel, beanInterface, null );
        if ( name == null )
        {
            throw new IllegalArgumentException( beanInterface
                                                + " is not a Neo4j Management Bean interface" );
        }
        return BeanProxy.load( getPlatformMBeanServer(), beanInterface, name );
    }

    static ObjectName getObjectName( KernelData kernel, Class<?> beanInterface, String beanName )
    {
        return getObjectName( kernel.instanceId(), beanInterface, beanName );
    }

    static ObjectName getObjectName( String instanceId, Class<?> beanInterface, String beanName )
    {
        final String name = beanName( beanInterface, beanName );
        if ( name == null ) return null;
        StringBuilder identifier = new StringBuilder( "org.neo4j:" );
        identifier.append( "instance=kernel#" );
        identifier.append( instanceId == null ? "*" : instanceId );
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

    private static String beanName( Class<?> beanInterface, String beanName )
    {
        final String name;
        if ( beanName != null )
        {
            name = beanName;
        }
        else if ( beanInterface == null )
        {
            name = "*";
        }
        else
        {
            try
            {
                name = (String) beanInterface.getField( "NAME" ).get( null );
            }
            catch ( Exception e )
            {
                return null;
            }
        }
        return name;
    }

    static ObjectName getObjectName( ObjectName beanQuery, Class<?> beanInterface, String beanName )
    {
        String name = beanName( beanInterface, beanName );
        if ( name == null ) return null;
        Hashtable<String, String> properties = new Hashtable<String, String>(
                beanQuery.getKeyPropertyList() );
        properties.put( "name", name );
        try
        {
            return new ObjectName( beanQuery.getDomain(), properties );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Could not create specified MBean Query." );
        }
    }
}
