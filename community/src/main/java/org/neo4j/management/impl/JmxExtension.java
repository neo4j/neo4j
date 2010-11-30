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

package org.neo4j.management.impl;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;

@Service.Implementation( KernelExtension.class )
public final class JmxExtension extends KernelExtension
{
    private static final Logger log = Logger.getLogger( JmxExtension.class.getName() );
    public JmxExtension()
    {
        super( "kernel jmx" );
    }

    @Override
    protected void load( KernelData kernel )
    {
        kernel.setState( this, loadBeans( kernel ) );
    }

    @Override
    protected void unload( KernelData kernel )
    {
        ( (JmxData) kernel.getState( this ) ).shutdown();
    }

    private JmxData loadBeans( KernelData kernel )
    {
        MBeanServer mbs = getPlatformMBeanServer();
        List<Neo4jMBean> beans = new ArrayList<Neo4jMBean>();
        for ( ManagementBeanProvider provider : Service.load( ManagementBeanProvider.class ) )
        {
            try
            {
                Neo4jMBean bean = provider.loadBeen( kernel );
                if ( bean != null )
                {
                    mbs.registerMBean( bean, bean.objectName );
                    beans.add( bean );
                }
            }
            catch ( Exception e )
            {
                log.info( "Failed to register JMX Bean " + provider );
            }
        }
        try
        {
            Neo4jMBean bean = new KernelBean( kernel );
            mbs.registerMBean( bean, bean.objectName );
            beans.add( bean );
        }
        catch ( Exception e )
        {
            log.info( "Failed to register Kernel JMX Bean" );
        }
        return new JmxData( kernel, beans.toArray( new Neo4jMBean[beans.size()] ) );
    }

    private static final class JmxData
    {
        private final Neo4jMBean[] beans;
        private final JMXServiceURL url;

        JmxData( KernelData kernel, Neo4jMBean[] beans )
        {
            this.beans = beans;
            @SuppressWarnings( "hiding" ) JMXServiceURL url = null;
            try
            {
                Class<?> cal = Class.forName( "sun.management.ConnectorAddressLink" );
                Method importRemote = cal.getMethod( "importRemoteFrom", int.class );
                @SuppressWarnings( "unchecked" ) Map<String, String> remote = (Map<String, String>) importRemote.invoke(
                        null, Integer.valueOf( 0 ) );
                Set<Integer> instances = new HashSet<Integer>();
                for ( String key : remote.keySet() )
                {
                    if ( key.startsWith( "sun.management.JMXConnectorServer" ) )
                    {
                        int end = key.lastIndexOf( '.' );
                        if ( end < 0 ) continue;
                        int start = key.lastIndexOf( '.', end );
                        if ( start < 0 ) continue;
                        final int id;
                        try
                        {
                            id = Integer.parseInt( key.substring( start, end ) );
                        }
                        catch ( NumberFormatException e )
                        {
                            continue;
                        }
                        instances.add( Integer.valueOf( id ) );
                    }
                }
                if ( !instances.isEmpty() )
                {
                    String prefix = "sun.management.JMXConnectorServer.";
                    if ( instances.size() > 1 )
                    {
                        for ( Object key : instances.toArray() )
                        {
                            if ( !remote.containsKey( "sun.management.JMXConnectorServer." + key
                                                      + ".remoteAddress" ) )
                            {
                                instances.remove( key );
                            }
                        }
                        if ( instances.contains( Integer.valueOf( 0 ) ) )
                        {
                            prefix = prefix + "0.";
                        }
                    }
                    if ( instances.size() == 1 )
                    {
                        String remoteAddress = remote.get( prefix + instances.iterator().next()
                                                           + "remoteAddress" );
                        url = new JMXServiceURL( remoteAddress );
                    }
                    else if ( !instances.isEmpty() )
                    {
                        // TODO: find the appropriate one
                    }
                }
            }
            catch ( LinkageError e )
            {
                log.log( Level.INFO, "Failed to load local JMX configuration.", e );
            }
            catch ( Exception e )
            {
                log.log( Level.INFO, "Failed to load local JMX configuration.", e );
            }
            if ( url == null )
            {
                Object portObj = kernel.getParam( "jmx.port" );
                int port = 0;
                if ( portObj instanceof Integer )
                {
                    port = ( (Integer) portObj ).intValue();
                }
                else if ( portObj instanceof String )
                {
                    try
                    {
                        port = Integer.parseInt( (String) portObj );
                    }
                    catch ( NumberFormatException ok )
                    {
                    }
                }
                if ( port > 0 )
                {
                    Object useSslObj = kernel.getParam( "jmx.use_ssl" );
                    boolean useSSL = false;
                    if ( useSslObj instanceof Boolean )
                    {
                        useSSL = ( (Boolean) useSslObj ).booleanValue();
                    }
                    else if ( useSslObj instanceof String )
                    {
                        useSSL = Boolean.parseBoolean( (String) useSslObj );
                    }
                    JMXConnectorServer server = createServer( port, useSSL );
                    if ( server != null )
                    {
                        try
                        {
                            server.getMBeanServer().registerMBean( server,
                                    getObjectName( kernel, null, "JMX Server" ) );
                        }
                        catch ( Exception e )
                        {
                            log.log( Level.INFO, "Failed to register MBean server as JMX bean", e );
                        }
                        url = server.getAddress();
                    }
                }
            }
            this.url = url;
        }

        void shutdown()
        {
            MBeanServer mbs = getPlatformMBeanServer();
            for ( Neo4jMBean bean : beans )
            {
                try
                {
                    mbs.unregisterMBean( bean.objectName );
                }
                catch ( Exception e )
                {
                    log.log( Level.WARNING, "Failed to unregister JMX Bean " + bean, e );
                }
            }
        }
    }

    JMXServiceURL getConnectionURL( KernelData kernel )
    {
        return ( (JmxData) kernel.getState( this ) ).url;
    }

    public static JMXConnectorServer createServer( int port, boolean useSSL )
    {
        MBeanServer server = getPlatformMBeanServer();
        final JMXServiceURL url;
        try
        {
            url = new JMXServiceURL( "rmi", null, port );
        }
        catch ( MalformedURLException e )
        {
            log.log( Level.WARNING, "Failed to start JMX Server", e );
            return null;
        }
        Map<String, Object> env = new HashMap<String, Object>();
        if ( useSSL )
        {
            env.put( RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                    new SslRMIClientSocketFactory() );
            env.put( RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                    new SslRMIServerSocketFactory() );
        }
        try
        {
            return JMXConnectorServerFactory.newJMXConnectorServer( url, env, server );
        }
        catch ( IOException e )
        {
            log.log( Level.WARNING, "Failed to start JMX Server", e );
            return null;
        }
    }

    public <T> T getBean( KernelData kernel, Class<T> beanInterface )
    {
        if ( !isLoaded( kernel ) ) throw new IllegalStateException( "Not Loaded!" );
        ObjectName name = getObjectName( kernel, beanInterface, null );
        if ( name == null )
        {
            throw new IllegalArgumentException( beanInterface
                                                + " is not a Neo4j Management Bean interface" );
        }
        return BeanProxy.load( getPlatformMBeanServer(), beanInterface, name );
    }

    static ObjectName getObjectName( KernelData kernel, Class<?> beanInterface, String beanName )
    {
        return BeanNaming.getObjectName( kernel.instanceId(), beanInterface, beanName );
    }
}
