/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ReflectionException;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.jmx.Description;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationChange;
import org.neo4j.kernel.configuration.ConfigurationChangeListener;

@Description( "The configuration parameters used to configure Neo4j" )
public final class ConfigurationBean extends Neo4jMBean
{
    public static final String CONFIGURATION_MBEAN_NAME = "Configuration";
    private final Map<String, String> config;

    private final Map<String, String> parameterDescriptions;

    ConfigurationBean( KernelData kernel, ManagementSupport support ) throws NotCompliantMBeanException
    {
        super( CONFIGURATION_MBEAN_NAME, kernel, support );
        this.config = new HashMap<>( kernel.getConfig().getParams() );
        Config configuration = kernel.getConfig();
        configuration.addConfigurationChangeListener( new UpdatedConfigurationListener() );

        Map<String, String> descriptions = new HashMap<>();

        for ( Class<?> settingsClass : configuration.getSettingsClasses() )
        {
            for ( final Field field : settingsClass.getFields() )
            {
                if ( Modifier.isStatic( field.getModifiers() ) && Modifier.isFinal( field.getModifiers() ) )
                {
                    final org.neo4j.graphdb.factory.Description documentation = field.getAnnotation( org.neo4j.graphdb.factory.Description.class );
                    if ( documentation == null || !Setting.class.isAssignableFrom(field.getType()) ) continue;
                    try
                    {
                        if ( !field.isAccessible() )
                        {
                            field.setAccessible( true );
                        }

                        String description = documentation.value();
                        Setting setting = (Setting) field.get( null );

                        descriptions.put( setting.name(), description );

                        String value = configuration.getParams().get( setting.name() );
                        if (value == null)
                        {
                            value = setting.getDefaultValue();
                        }
                        config.put( setting.name(), value );
                    }
                    catch ( Exception ignore )
                    {
                    }
                }
            }
        }
        parameterDescriptions = Collections.unmodifiableMap( descriptions );

    }

    private String describeConfigParameter( String param )
    {
        String description = parameterDescriptions.get( param );
        return description != null ? description : "Configuration attribute";
    }

    private MBeanAttributeInfo[] keys()
    {
        List<MBeanAttributeInfo> keys = new ArrayList<>();
        for ( String key : config.keySet() )
        {
            keys.add( new MBeanAttributeInfo( key, String.class.getName(),
                                              describeConfigParameter( key ), true, false, false ) );
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
    public void setAttribute( Attribute attribute )
        throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException
    {
        throw new InvalidAttributeValueException( "Not a writeable attribute: " + attribute.getName() );
    }

    @Override
    public MBeanInfo getMBeanInfo()
    {
        Description description = getClass().getAnnotation( Description.class );
        return new MBeanInfo( getClass().getName(), description != null ? description.value() : "Neo4j configuration",
                keys(), null, null, null );
    }

    @Override
    public Object invoke( String s, Object[] objects, String[] strings )
        throws MBeanException, ReflectionException
    {
        try
        {
            return getClass().getMethod( s ).invoke( this );
        }
        catch( InvocationTargetException e )
        {
            throw new MBeanException( (Exception) e.getTargetException() );
        }
        catch( Exception e )
        {
            throw new MBeanException( e );
        }
    }

    private class UpdatedConfigurationListener
        implements ConfigurationChangeListener
    {
        @Override
        public void notifyConfigurationChanges( Iterable<ConfigurationChange> change )
        {
            for( ConfigurationChange configurationChange : change )
            {
                config.put( configurationChange.getName(), configurationChange.getNewValue() );
            }
        }
    }
}
