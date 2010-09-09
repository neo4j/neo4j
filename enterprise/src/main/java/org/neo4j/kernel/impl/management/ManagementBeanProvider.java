package org.neo4j.kernel.impl.management;

import javax.management.DynamicMBean;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;

public abstract class ManagementBeanProvider extends Service
{
    final Class<?> beanInterface;
    private final String beanName;

    protected ManagementBeanProvider( Class<?> beanInterface )
    {
        this( beanName( beanInterface ), beanInterface );
    }

    ManagementBeanProvider( String beanName )
    {
        this( beanName, DynamicMBean.class );
    }

    private ManagementBeanProvider( String beanName, Class<?> beanInterface )
    {
        super( beanName );
        this.beanName = beanName;
        this.beanInterface = beanInterface;
    }

    private static String beanName( Class<?> iface )
    {
        if ( iface.isInterface() )
        {
            try
            {
                return (String) iface.getField( "NAME" ).get( null );
            }
            catch ( Exception fallthrough )
            {
            }
        }
        throw new IllegalArgumentException( iface + " is not a Neo4j Management Been interface" );
    }

    protected abstract Neo4jMBean createMBean( KernelData kernel )
            throws NotCompliantMBeanException;

    protected Neo4jMBean createMXBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return createMBean( kernel );
    }

    Neo4jMBean loadBeen( KernelData kernel )
    {
        try
        {
            if ( BeanProxy.supportsMxBeans() )
            {
                return createMXBean( kernel );
            }
            else
            {
                return createMBean( kernel );
            }
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    ObjectName getObjectName( KernelData kernel )
    {
        ObjectName name = JmxExtension.getObjectName( kernel, beanInterface, beanName );
        if ( name == null )
        {
            throw new IllegalArgumentException( beanInterface
                                                + " is not a Neo4j Management Bean interface" );
        }
        return name;
    }
}
