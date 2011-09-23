/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.MBeanServer;
import javax.management.remote.JMXServiceURL;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

@Service.Implementation( KernelExtension.class )
public final class JmxExtension extends KernelExtension<JmxExtension.JmxData>
{
    public static final String KEY = "kernel jmx";
    private static final Logger log = Logger.getLogger( JmxExtension.class.getName() );

    public JmxExtension()
    {
        super( KEY );
    }

    @Override
    protected JmxData load( KernelData kernel )
    {
        ManagementSupport support = ManagementSupport.load();
        MBeanServer mbs = support.getMBeanServer();
        List<Neo4jMBean> beans = new LinkedList<Neo4jMBean>();
        try
        {
            Neo4jMBean bean = new KernelBean( kernel, support );
            mbs.registerMBean( bean, bean.objectName );
            beans.add( bean );
        }
        catch ( Exception e )
        {
            log.info( "Failed to register Kernel JMX Bean" );
        }
        for ( ManagementBeanProvider provider : Service.load( ManagementBeanProvider.class ) )
        {
            try
            {
                for ( Neo4jMBean bean : provider.loadBeans( kernel, support ) )
                {
                    mbs.registerMBean( bean, bean.objectName );
                    beans.add( bean );
                }
            }
            catch ( Exception e )
            { // Unexpected exception
                log.info( "Failed to register JMX Bean " + provider + " (" + e + ")" );
            }
        }
        try
        {
            Neo4jMBean bean = new ConfigurationBean( kernel, support );
            mbs.registerMBean( bean, bean.objectName );
            beans.add( bean );
        }
        catch ( Exception e )
        {
            log.info( "Failed to register Configuration JMX Bean" );
        }
        return new JmxData( kernel, support, beans.toArray( new Neo4jMBean[beans.size()] ) );
    }

    @Override
    protected void unload( JmxData data )
    {
        data.shutdown();
    }

    public static final class JmxData
    {
        private final Neo4jMBean[] beans;
        private final JMXServiceURL url;
        private final ManagementSupport support;

        private JmxData( KernelData kernel, ManagementSupport support, Neo4jMBean[] beans )
        {
            this.support = support;
            this.beans = beans;
            this.url = support.getJMXServiceURL( kernel );
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

        /**
         * Used through reflection {@link org.neo4j.kernel.EmbeddedGraphDbImpl#getManagementBeans(Class) from kernel}.
         */
        public <T> Collection<T> getManagementBeans( Class<T> beanInterface )
        {
            Collection<T> result = null;
            if ( support.getClass() != ManagementSupport.class && beans.length > 0 && beans[0] instanceof KernelBean )
            {
                try
                {
                    result = support.getProxiesFor( beanInterface, (KernelBean) beans[0] );
                }
                catch ( UnsupportedOperationException ignore )
                {
                    result = null; // go to fall back
                }
            }
            if ( result == null )
            {
                // Fall back: if we cannot create proxy, we can search for instances
                result = new ArrayList<T>();
                for ( Neo4jMBean bean : beans )
                {
                    if ( beanInterface.isInstance( bean ) ) result.add( beanInterface.cast( bean ) );
                }
            }
            return result;
        }
    }

    public JMXServiceURL getConnectionURL( KernelData kernel )
    {
        return getState( kernel ).url;
    }
}
