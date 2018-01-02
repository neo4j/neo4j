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

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.Kernel;
import org.neo4j.kernel.KernelData;

public abstract class Neo4jMBean extends StandardMBean
{
    final ObjectName objectName;

    protected Neo4jMBean( ManagementData management, boolean isMXBean, String... extraNaming )
    {
        super( management.provider.beanInterface, isMXBean );
        management.validate( getClass() );
        this.objectName = management.getObjectName( extraNaming );
    }

    protected Neo4jMBean( ManagementData management, String... extraNaming )
            throws NotCompliantMBeanException
    {
        super( management.provider.beanInterface );
        management.validate( getClass() );
        this.objectName = management.getObjectName( extraNaming );
    }

    /** Constructor for {@link KernelBean} */
    Neo4jMBean( Class<Kernel> beanInterface, KernelData kernel, ManagementSupport support )
            throws NotCompliantMBeanException
    {
        super( beanInterface );
        this.objectName = support.createObjectName( kernel.instanceId(), beanInterface );
    }

    /** Constructor for {@link ConfigurationBean} */
    Neo4jMBean( String beanName, KernelData kernel, ManagementSupport support )
            throws NotCompliantMBeanException
    {
        super( DynamicMBean.class );
        this.objectName = support.createObjectName( kernel.instanceId(), beanName, false );
    }

    @Override
    protected String getClassName( MBeanInfo info )
    {
        final Class<?> iface = this.getMBeanInterface();
        return iface == null ? super.getClassName( info ) : iface.getName();
    }

    @Override
    protected String getDescription( MBeanInfo info )
    {
        Description description = describeClass();
        if ( description != null ) return description.value();
        return super.getDescription( info );
    }

    @Override
    protected String getDescription( MBeanAttributeInfo info )
    {
        Description description = describeMethod( info, "get", "is" );
        if ( description != null ) return description.value();
        return super.getDescription( info );
    }

    @Override
    protected String getDescription( MBeanOperationInfo info )
    {
        Description description = describeMethod( info );
        if ( description != null ) return description.value();
        return super.getDescription( info );
    }

    @Override
    protected int getImpact( MBeanOperationInfo info )
    {
        Description description = describeMethod( info );
        if ( description != null ) return description.impact();
        return super.getImpact( info );
    }

    private Description describeClass()
    {
        Description description = getClass().getAnnotation( Description.class );
        if ( description == null )
        {
            for ( Class<?> iface : getClass().getInterfaces() )
            {
                description = iface.getAnnotation( Description.class );
                if ( description != null ) break;
            }
        }
        return description;
    }

    private Description describeMethod( MBeanFeatureInfo info, String... prefixes )
    {
        Description description = describeMethod( getClass(), info.getName(), prefixes );
        if ( description == null )
        {
            for ( Class<?> iface : getClass().getInterfaces() )
            {
                description = describeMethod( iface, info.getName(), prefixes );
                if ( description != null ) break;
            }
        }
        return description;
    }

    private static Description describeMethod( Class<?> type, String methodName, String[] prefixes )
    {
        if ( prefixes == null || prefixes.length == 0 )
        {
            try
            {
                return type.getMethod( methodName ).getAnnotation( Description.class );
            }
            catch ( Exception e )
            {
                return null;
            }
        }
        else
        {
            for ( String prefix : prefixes )
            {
                try
                {
                    return type.getMethod( prefix + methodName ).getAnnotation( Description.class );
                }
                catch ( Exception e )
                {
                    // continue to next
                }
            }
            return null;
        }
    }
}
