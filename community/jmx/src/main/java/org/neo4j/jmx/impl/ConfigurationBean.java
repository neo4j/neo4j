/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.jmx.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ReflectionException;

import org.neo4j.jmx.Description;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.impl.annotations.Documented;

@Description( "The configuration parameters used to configure Neo4j" )
public final class ConfigurationBean extends Neo4jMBean
{
    public static final String CONFIGURATION_MBEAN_NAME = "Configuration";
    private final Map<String, String> config;

    ConfigurationBean( KernelData kernel, ManagementSupport support ) throws NotCompliantMBeanException
    {
        super( CONFIGURATION_MBEAN_NAME, kernel, support );
        this.config = kernel.getConfigParams();
    }

    private static final Map<String, String> parameterDescriptions;
    static
    {
        final Map<String, String> descriptions = new HashMap<String, String>();
        for ( final Field field : Config.class.getFields() )
        {
            if ( Modifier.isStatic( field.getModifiers() ) && Modifier.isFinal( field.getModifiers() ) )
            {
                final Documented documentation = field.getAnnotation( Documented.class );
                if ( documentation == null || field.getType() != String.class ) continue;
                try
                {
                    if ( !field.isAccessible() ) field.setAccessible( true );
                    descriptions.put( (String) field.get( null ), documentation.value() );
                }
                catch ( Exception e )
                {
                    continue;
                }
            }
        }
        parameterDescriptions = Collections.unmodifiableMap( descriptions );
    }

    private static String describeConfigParameter( String param )
    {
        String description = parameterDescriptions.get( param );
        return description != null ? description : "Configuration attribute";
    }

    private MBeanAttributeInfo[] keys()
    {
        List<MBeanAttributeInfo> keys = new ArrayList<MBeanAttributeInfo>();
        for ( Map.Entry<String, String> entry : config.entrySet() )
        {
            if ( entry.getKey() instanceof String )
            {
                keys.add( new MBeanAttributeInfo( (String) entry.getKey(), String.class.getName(),
                        describeConfigParameter( (String) entry.getKey() ), true, false, false ) );
            }
        }
        return keys.toArray( new MBeanAttributeInfo[keys.size()] );
    }

    @Override
    public Object getAttribute( String attribute ) throws AttributeNotFoundException, MBeanException,
            ReflectionException
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
                result.add( new Attribute( attribute, getAttribute( attribute ) ) );
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
        return new MBeanInfo( getClass().getName(), description != null ? description.value() : "Neo4j configuration",
                keys(), null, null, null );
    }
}
