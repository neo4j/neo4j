/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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

import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.management.impl.ConfigurationBean;
import org.neo4j.management.impl.KernelProxy;

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
        return get( connect( url, null, null ) );
    }

    private static MBeanServerConnection connect( JMXServiceURL url, String username,
            String password )
    {
        Map<String, Object> environment = new HashMap<String, Object>();
        if ( username != null && password != null )
        {
            environment.put( JMXConnector.CREDENTIALS, new String[] { username, password } );
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
            return get( server, server.queryNames( getObjectName( Kernel.class, null ), null ) );
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
            return get( server, server.queryNames( getObjectName( Kernel.class, kernelIdentifier ),
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
            Set<ObjectName> kernels = server.queryNames( getObjectName( Kernel.class, null ), null );
            Neo4jManager[] managers = new Neo4jManager[kernels.size()];
            Iterator<ObjectName> it = kernels.iterator();
            for ( int i = 0; i < managers.length; i++ )
            {
                managers[i] = new Neo4jManager( server, proxy( server, Kernel.class, it.next() ) );
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
            return new Neo4jManager( server,
                    proxy( server, Kernel.class, kernels.iterator().next() ) );
        }
        else
        {
            throw new NoSuchElementException(
                    "Too many matching Neo4j Graph Databases running on server" );
        }
    }

    private final ObjectName config;

    public Neo4jManager( Kernel kernel )
    {
        this( getServer( kernel ), actual( kernel ) );
    }

    private static MBeanServerConnection getServer( Kernel kernel )
    {
        if ( kernel instanceof Proxy )
        {
            InvocationHandler handler = Proxy.getInvocationHandler( kernel );
            if ( handler instanceof MBeanServerInvocationHandler )
            {
                return ( (MBeanServerInvocationHandler) handler ).getMBeanServerConnection();
            }
        }
        else if ( kernel instanceof Neo4jManager )
        {
            return ( (Neo4jManager) kernel ).server;
        }
        throw new UnsupportedOperationException( "Cannot get server for kernel: " + kernel );
    }

    private static Kernel actual( Kernel kernel )
    {
        return kernel instanceof Neo4jManager ? ( (Neo4jManager) kernel ).kernel : kernel;
    }

    private Neo4jManager( MBeanServerConnection server, Kernel kernel )
    {
        super( server, kernel );
        this.config = getObjectName( ConfigurationBean.CONFIGURATION_MBEAN_NAME );
    }

    public Cache getCacheBean()
    {
        return getBean( Cache.class );
    }

    public LockManager getLockManagerBean()
    {
        return getBean( LockManager.class );
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

    public XaManager getXaManagerBean()
    {
        return getBean( XaManager.class );
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
    public <T> T getBean( Class<T> beanInterface )
    {
        return super.getBean( beanInterface );
    }

    public Date getKernelStartTime()
    {
        return kernel.getKernelStartTime();
    }

    public String getKernelVersion()
    {
        return kernel.getKernelVersion();
    }

    public ObjectName getMBeanQuery()
    {
        return kernel.getMBeanQuery();
    }

    public Date getStoreCreationDate()
    {
        return kernel.getStoreCreationDate();
    }

    public String getStoreDirectory()
    {
        return kernel.getStoreDirectory();
    }

    public String getStoreId()
    {
        return kernel.getStoreId();
    }

    public long getStoreLogVersion()
    {
        return kernel.getStoreLogVersion();
    }

    public boolean isReadOnly()
    {
        return kernel.isReadOnly();
    }

    public static JMXServiceURL getConnectionURL( KernelData kernel )
    {
        return KernelProxy.getConnectionURL( kernel );
    }
}
