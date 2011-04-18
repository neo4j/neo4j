/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management.impl;

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
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ReflectionException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.Config;

@Service.Implementation( ManagementBeanProvider.class )
public final class ConfigurationBean extends ManagementBeanProvider
{
    public static final String CONFIGURATION_MBEAN_NAME = "Configuration";

    @Description( "Tell Neo4j to use memory mapped buffers for accessing the native storage layer" )
    public static final String USE_MEMORY_MAPPED_BUFFERS = Config.USE_MEMORY_MAPPED_BUFFERS;
    @Description( "Print out the effective Neo4j configuration after startup" )
    public static final String DUMP_CONFIGURATION = Config.DUMP_CONFIGURATION;
    @Description( "Make Neo4j keep the logical transaction logs for being able to backup the database" )
    public static final String KEEP_LOGICAL_LOGS = Config.KEEP_LOGICAL_LOGS;
    @Description( "Enable a remote shell server which shell clients can log in to" )
    public static final String ENABLE_REMOTE_SHELL = Config.ENABLE_REMOTE_SHELL;
    @Description( "<TODO: document this>" )
    public static final String BACKUP_SLAVE = Config.BACKUP_SLAVE;
    @Description( "Only allow read operations from this Neo4j instance" )
    public static final String READ_ONLY = Config.READ_ONLY;
    @Description( "Relative path for where the Neo4j storage directory is located" )
    public static final String STORAGE_DIRECTORY = Config.STORAGE_DIRECTORY;
    @Description( "Use a quick approach for rebuilding the ID generators. "
                  + "This give quicker recovery time, but will limit the ability to reuse the space of deleted entities." )
    public static final String REBUILD_IDGENERATORS_FAST = Config.REBUILD_IDGENERATORS_FAST;
    @Description( "The size to allocate for memory mapping the node store" )
    public static final String NODE_STORE_MMAP_SIZE = Config.NODE_STORE_MMAP_SIZE;
    @Description( "The size to allocate for memory mapping the array property store" )
    public static final String ARRAY_PROPERTY_STORE_MMAP_SIZE = Config.ARRAY_PROPERTY_STORE_MMAP_SIZE;
    @Description( "The size to allocate for memory mapping the store for property key strings" )
    public static final String PROPERTY_INDEX_KEY_STORE_MMAP_SIZE = Config.PROPERTY_INDEX_KEY_STORE_MMAP_SIZE;
    @Description( "The size to allocate for memory mapping the store for property key indexes" )
    public static final String PROPERTY_INDEX_STORE_MMAP_SIZE = Config.PROPERTY_INDEX_STORE_MMAP_SIZE;
    @Description( "The size to allocate for memory mapping the property value store" )
    public static final String PROPERTY_STORE_MMAP_SIZE = Config.PROPERTY_STORE_MMAP_SIZE;
    @Description( "The size to allocate for memory mapping the string property store" )
    public static final String STRING_PROPERTY_STORE_MMAP_SIZE = Config.STRING_PROPERTY_STORE_MMAP_SIZE;
    @Description( "The size to allocate for memory mapping the relationship store" )
    public static final String RELATIONSHIP_STORE_MMAP_SIZE = Config.RELATIONSHIP_STORE_MMAP_SIZE;
    @Description( "Relative path for where the Neo4j logical log is located" )
    public static final String LOGICAL_LOG = Config.LOGICAL_LOG;
    @Description( "Relative path for where the Neo4j storage information file is located" )
    public static final String NEO_STORE = Config.NEO_STORE;
    @Description( "The type of cache to use for nodes and relationships, one of [weak, soft, none]" )
    public static final String CACHE_TYPE = Config.CACHE_TYPE;

    public ConfigurationBean()
    {
        super( ConfigurationInterface.class );
    }

    @ManagementInterface( name = CONFIGURATION_MBEAN_NAME )
    private interface ConfigurationInterface extends DynamicMBean
    {
        // a hack to conform to the constructor of the ManagementBeanProvider
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new ConfigurationImpl( management );
    }

    @Description( "The configuration parameters used to configure Neo4j" )
    private static class ConfigurationImpl extends Neo4jMBean
    {
        private static final Map<String, String> parameterDescriptions;
        static
        {
            final Map<String, String> descriptions = new HashMap<String, String>();
            for ( final Field field : ConfigurationBean.class.getFields() )
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

        protected ConfigurationImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.config = management.getKernelData().getConfigParams();
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
            return new MBeanInfo( getClass().getName(), description != null ? description.value()
                    : "Neo4j configuration", keys(), null, null, null );
        }
    }
}
