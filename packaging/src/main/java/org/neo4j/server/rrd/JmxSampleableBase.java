package org.neo4j.server.rrd;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.management.Kernel;
import org.neo4j.management.Primitives;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.lang.management.ManagementFactory;

public abstract class JmxSampleableBase  implements Sampleable
{
    private MBeanServer mbeanServer;
    private ObjectName objectName;

    public JmxSampleableBase( AbstractGraphDatabase graphDb ) throws MalformedObjectNameException
    {
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName neoQuery = graphDb.getManagementBean( Kernel.class ).getMBeanQuery();
        String instance = neoQuery.getKeyProperty( "instance" );
        String baseName = neoQuery.getDomain() + ":instance=" + instance + ",name=";
        objectName = new ObjectName( baseName + Primitives.NAME );
    }

    public abstract String getName();

    public long getValue()
    {
        try
        {
            return (Long)mbeanServer.getAttribute( objectName, getJmxAttributeName() );
        } catch ( MBeanException e )
        {
            throw new RuntimeException( e );
        } catch ( AttributeNotFoundException e )
        {
            throw new RuntimeException( e );
        } catch ( InstanceNotFoundException e )
        {
            throw new RuntimeException( e );
        } catch ( ReflectionException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected abstract String getJmxAttributeName();
}
