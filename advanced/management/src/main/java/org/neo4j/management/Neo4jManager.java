/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.management;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.Primitives;
import org.neo4j.jmx.StoreFile;
import org.neo4j.jmx.impl.ConfigurationBean;
import org.neo4j.management.impl.KernelProxy;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

public final class Neo4jManager extends KernelProxy implements Kernel
{
    public static Neo4jManager get()
    {
        return get( getPlatformMBeanServer() );
    }

    public static Neo4jManager get( String kernelIdentifier )
    {
        return get( getPlatformMBeanServer(), kernelIdentifier );
    }

    public static Neo4jManager get( JMXServiceURL url )
    {
        return get( connect( url, null, null ) );
    }

    public static Neo4jManager get( JMXServiceURL url, String kernelIdentifier )
    {
        return get( connect( url, null, null ), kernelIdentifier );
    }

    public static Neo4jManager get( JMXServiceURL url, String username, String password )
    {
        return get( connect( url, username, password ) );
    }

    public static Neo4jManager get( JMXServiceURL url, String username, String password,
                                    String kernelIdentifier )
    {
        return get( connect( url, username, password ), kernelIdentifier );
    }

    private static MBeanServerConnection connect( JMXServiceURL url, String username,
                                                  String password )
    {
        Map<String, Object> environment = new HashMap<String, Object>();
        if ( username != null && password != null )
        {
            environment.put( JMXConnector.CREDENTIALS, new String[]{username, password} );
        }
        else if ( username != password )
        {
            throw new IllegalArgumentException(
                    "User name and password must either both be specified, or both be null." );
        }
        try
        {
            try
            {
                return JMXConnectorFactory.connect( url, environment ).getMBeanServerConnection();
            }
            catch ( SecurityException e )
            {
                environment.put( RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                        new SslRMIClientSocketFactory() );
                return JMXConnectorFactory.connect( url, environment ).getMBeanServerConnection();
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Connection failed.", e );
        }
    }

    public static Neo4jManager get( MBeanServerConnection server )
    {
        server.getClass();
        try
        {
            return get( server, server.queryNames( createObjectName( "*", Kernel.class ), null ) );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Connection failed.", e );
        }
    }

    public static Neo4jManager get( MBeanServerConnection server, String kernelIdentifier )
    {
        server.getClass();
        kernelIdentifier.getClass();
        try
        {
            return get( server, server.queryNames( createObjectName( kernelIdentifier, Kernel.class ),
                    null ) );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Connection failed.", e );
        }
    }

    public static Neo4jManager[] getAll( MBeanServerConnection server )
    {
        try
        {
            Set<ObjectName> kernels = server.queryNames( createObjectName( "*", Kernel.class ), null );
            Neo4jManager[] managers = new Neo4jManager[kernels.size()];
            Iterator<ObjectName> it = kernels.iterator();
            for ( int i = 0; i < managers.length; i++ )
            {
                managers[i] = new Neo4jManager( server, it.next() );
            }
            return managers;
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Connection failed.", e );
        }
    }

    private static Neo4jManager get( MBeanServerConnection server, Collection<ObjectName> kernels )
    {
        if ( kernels.size() == 0 )
        {
            throw new NoSuchElementException( "No matching Neo4j Graph Database running on server" );
        }
        else if ( kernels.size() == 1 )
        {
            return new Neo4jManager( server, kernels.iterator().next() );
        }
        else
        {
            throw new NoSuchElementException(
                    "Too many matching Neo4j Graph Databases running on server" );
        }
    }

    private final ObjectName config;
    private final Kernel proxy;

    public Neo4jManager( Kernel kernel )
    {
        this( getServer( kernel ), getName( kernel ) );
    }

    private static MBeanServerConnection getServer( Kernel kernel )
    {
        if ( kernel instanceof Proxy )
        {
            InvocationHandler handler = Proxy.getInvocationHandler( kernel );
            if ( handler instanceof MBeanServerInvocationHandler )
            {
                return ((MBeanServerInvocationHandler) handler).getMBeanServerConnection();
            }
        }
        else if ( kernel instanceof Neo4jManager )
        {
            return ((Neo4jManager) kernel).server;
        }
        throw new UnsupportedOperationException( "Cannot get server for kernel: " + kernel );
    }

    private static ObjectName getName( Kernel kernel )
    {
        if ( kernel instanceof Proxy )
        {
            InvocationHandler handler = Proxy.getInvocationHandler( kernel );
            if ( handler instanceof MBeanServerInvocationHandler )
            {
                return ((MBeanServerInvocationHandler) handler).getObjectName();
            }
        }
        else if ( kernel instanceof Neo4jManager )
        {
            return ((Neo4jManager) kernel).kernel;
        }
        throw new UnsupportedOperationException( "Cannot get name for kernel: " + kernel );
    }

    private Neo4jManager( MBeanServerConnection server, ObjectName kernel )
    {
        super( server, kernel );
        this.config = createObjectName( ConfigurationBean.CONFIGURATION_MBEAN_NAME );
        this.proxy = getBean( Kernel.class );
    }

    public Collection<Cache> getCacheBeans()
    {
        return getBeans( Cache.class );
    }

    public LockManager getLockManagerBean()
    {
        return getBean( LockManager.class );
    }

    public IndexSamplingManager getIndexSamplingManagerBean()
    {
        return getBean( IndexSamplingManager.class );
    }

    public MemoryMapping getMemoryMappingBean()
    {
        return getBean( MemoryMapping.class );
    }

    public Primitives getPrimitivesBean()
    {
        return getBean( Primitives.class );
    }

    public StoreFile getStoreFileBean()
    {
        return getBean( StoreFile.class );
    }

    public TransactionManager getTransactionManagerBean()
    {
        return getBean( TransactionManager.class );
    }

    public PageCache getPageCacheBean()
    {
        return getBean( PageCache.class );
    }

    public HighAvailability getHighAvailabilityBean()
    {
        return getBean( HighAvailability.class );
    }

    public BranchedStore getBranchedStoreBean()
    {
        return getBean( BranchedStore.class );
    }

    public Object getConfigurationParameter( String key )
    {
        try
        {
            return server.getAttribute( config, key );
        }
        catch ( AttributeNotFoundException e )
        {
            return null;
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Could not access the configuration bean", e );
        }
    }

    public Map<String, Object> getConfiguration()
    {
        final String[] keys;
        final AttributeList attributes;
        try
        {
            MBeanAttributeInfo[] keyInfo = server.getMBeanInfo( config ).getAttributes();
            keys = new String[keyInfo.length];
            for ( int i = 0; i < keys.length; i++ )
            {
                keys[i] = keyInfo[i].getName();
            }
            attributes = server.getAttributes( config, keys );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Could not access the configuration bean", e );
        }
        Map<String, Object> configuration = new HashMap<String, Object>();
        for ( int i = 0; i < keys.length; i++ )
        {
            configuration.put( keys[i], attributes.get( i ) );

        }
        return configuration;
    }

    @Override
    public List<Object> allBeans()
    {
        List<Object> beans = super.allBeans();
        @SuppressWarnings("hiding") Kernel kernel = null;
        for ( Object bean : beans )
        {
            if ( bean instanceof Kernel )
            {
                kernel = (Kernel) bean;
            }
        }
        if ( kernel != null )
        {
            beans.remove( kernel );
        }
        return beans;
    }

    @Override
    public Date getKernelStartTime()
    {
        return proxy.getKernelStartTime();
    }

    @Override
    public String getKernelVersion()
    {
        return proxy.getKernelVersion();
    }

    @Override
    public ObjectName getMBeanQuery()
    {
        return proxy.getMBeanQuery();
    }

    @Override
    public Date getStoreCreationDate()
    {
        return proxy.getStoreCreationDate();
    }

    @Override
    public String getStoreDirectory()
    {
        return proxy.getStoreDirectory();
    }

    @Override
    public String getStoreId()
    {
        return proxy.getStoreId();
    }

    @Override
    public long getStoreLogVersion()
    {
        return proxy.getStoreLogVersion();
    }

    @Override
    public boolean isReadOnly()
    {
        return proxy.isReadOnly();
    }
}
