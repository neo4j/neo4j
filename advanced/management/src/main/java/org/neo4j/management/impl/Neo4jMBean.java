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

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.management.Kernel;

public class Neo4jMBean extends StandardMBean
{
    final ObjectName objectName;

    protected Neo4jMBean( ManagementBeanProvider provider, KernelData kernel, boolean isMXBean )
    {
        super( provider.beanInterface, isMXBean );
        this.objectName = provider.getObjectName( kernel );
    }

    protected Neo4jMBean( ManagementBeanProvider provider, KernelData kernel )
            throws NotCompliantMBeanException
    {
        super( provider.beanInterface );
        this.objectName = provider.getObjectName( kernel );
    }

    Neo4jMBean( Class<Kernel> beenInterface, KernelData kernel ) throws NotCompliantMBeanException
    {
        super( beenInterface );
        this.objectName = JmxExtension.getObjectName( kernel, beenInterface, null );
    }

    @Override
    protected String getClassName( MBeanInfo info )
    {
        final Class<?> iface = this.getMBeanInterface();
        return iface == null ? info.getClassName() : iface.getName();
    }

    @Override
    protected String getDescription( MBeanInfo info )
    {
        Description description = getClass().getAnnotation( Description.class );
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

    private Description describeMethod( MBeanFeatureInfo info, String... prefixes )
    {
        if ( prefixes == null || prefixes.length == 0 )
        {
            try
            {
                return getClass().getMethod( info.getName() ).getAnnotation( Description.class );
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
                    return getClass().getMethod( prefix + info.getName() ).getAnnotation(
                            Description.class );
                }
                catch ( Exception e )
                {
                }
            }
            return null;
        }
    }
}
