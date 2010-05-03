package org.neo4j.kernel.manage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;

class Configuration extends Neo4jJmx implements DynamicMBean
{
    private final Map<Object, Object> config;

    Configuration( int instanceId, Map<Object, Object> config )
    {
        super( instanceId );
        this.config = config;
    }

    private MBeanAttributeInfo[] keys()
    {
        List<MBeanAttributeInfo> keys = new ArrayList<MBeanAttributeInfo>();
        for ( Map.Entry<Object, Object> entry : config.entrySet() )
        {
            if ( entry.getKey() instanceof String )
            {
                keys.add( new MBeanAttributeInfo( (String) entry.getKey(),
                        String.class.getName(), "Configuration attribute",
                        true, false, false ) );
            }
        }
        return keys.toArray( new MBeanAttributeInfo[keys.size()] );
    }

    public Object getAttribute( String attribute )
            throws AttributeNotFoundException, MBeanException,
            ReflectionException
    {
        return config.get( attribute );
    }

    public AttributeList getAttributes( String[] attributes )
    {
        AttributeList result = new AttributeList( attributes.length );
        for ( String attribute : attributes )
        {
            try
            {
                result.add( getAttribute( attribute ) );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
        return result;
    }

    public MBeanInfo getMBeanInfo()
    {
        return new MBeanInfo( getClass().getName(), "Neo4j configuration",
                keys(), null, null, null );
    }

    public Object invoke( String actionName, Object[] params, String[] signature )
            throws MBeanException, ReflectionException
    {
        throw new MBeanException( new NoSuchMethodException( actionName ) );
    }

    public void setAttribute( Attribute attribute )
            throws AttributeNotFoundException, InvalidAttributeValueException,
            MBeanException, ReflectionException
    {
    }

    public AttributeList setAttributes( AttributeList attributes )
    {
        return getAttributes( new String[0] );
    }
}
