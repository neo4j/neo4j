package org.neo4j.management.impl;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.management.Kernel;

public class Neo4jMBean extends StandardMBean
{
    final ObjectName objectName;

    protected Neo4jMBean( ManagementBeanProvider provider, KernelData kernel, boolean isMXBean )
    {
        super( provider.beanInterface, isMXBean );
        this.objectName = provider.getObjectName( kernel );
    }

    protected Neo4jMBean( ManagementBeanProvider provider, KernelData kernel )
            throws NotCompliantMBeanException
    {
        super( provider.beanInterface );
        this.objectName = provider.getObjectName( kernel );
    }

    Neo4jMBean( Class<Kernel> beenInterface, KernelData kernel ) throws NotCompliantMBeanException
    {
        super( beenInterface );
        this.objectName = JmxExtension.getObjectName( kernel, beenInterface, null );
    }

    @Override
    protected String getClassName( MBeanInfo info )
    {
        final Class<?> iface = this.getMBeanInterface();
        return iface == null ? info.getClassName() : iface.getName();
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
