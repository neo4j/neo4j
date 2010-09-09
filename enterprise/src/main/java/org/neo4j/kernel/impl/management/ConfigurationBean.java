package org.neo4j.kernel.impl.management;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ReflectionException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelExtension.KernelData;

@Service.Implementation( ManagementBeanProvider.class )
public final class ConfigurationBean extends ManagementBeanProvider
{
    public ConfigurationBean()
    {
        super( "Configuration" );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new ConfigurationImpl( this, kernel );
    }

    @Description( "The configuration parameters used to configure Neo4j" )
    private static class ConfigurationImpl extends Neo4jMBean
    {

        private static final Map<String, String> parameterDescriptions;
        static
        {
            final Map<String, String> descriptions = new HashMap<String, String>();
            for ( final Field field : Config.class.getFields() )
            {
                if ( Modifier.isStatic( field.getModifiers() )
                     && Modifier.isFinal( field.getModifiers() ) )
                {
                    final Description descr = field.getAnnotation( Description.class );
                    if ( descr == null || field.getType() != String.class ) continue;
                    try
                    {
                        if ( !field.isAccessible() ) field.setAccessible( true );
                        descriptions.put( (String) field.get( null ), descr.value() );
                    }
                    catch ( Exception e )
                    {
                        continue;
                    }
                }
            }
            parameterDescriptions = Collections.unmodifiableMap( descriptions );
        }
        private final Map<Object, Object> config;

        protected ConfigurationImpl( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.config = kernel.getConfigParams();
        }

        private static String describeConfigParameter( String param )
        {
            String description = parameterDescriptions.get( param );
            return description != null ? description : "Configuration attribute";
        }

        private MBeanAttributeInfo[] keys()
        {
            List<MBeanAttributeInfo> keys = new ArrayList<MBeanAttributeInfo>();
            for ( Map.Entry<Object, Object> entry : config.entrySet() )
            {
                if ( entry.getKey() instanceof String )
                {
                    keys.add( new MBeanAttributeInfo( (String) entry.getKey(),
                            String.class.getName(),
                            describeConfigParameter( (String) entry.getKey() ), true, false, false ) );
                }
            }
            return keys.toArray( new MBeanAttributeInfo[keys.size()] );
        }

        @Override
        public Object getAttribute( String attribute ) throws AttributeNotFoundException,
                MBeanException, ReflectionException
        {
            return config.get( attribute );
        }

        @Override
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

        @Override
        public MBeanInfo getMBeanInfo()
        {
            Description description = getClass().getAnnotation( Description.class );
            return new MBeanInfo( getClass().getName(), description != null ? description.value()
                    : "Neo4j configuration", keys(), null, null, null );
        }
    }
}
