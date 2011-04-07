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

import javax.management.DynamicMBean;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelData;

public abstract class ManagementBeanProvider extends Service
{
    final Class<?> beanInterface;

    public ManagementBeanProvider( Class<?> beanInterface )
    {
        super( ManagementSupport.beanName( beanInterface ) );
        if ( DynamicMBean.class.isAssignableFrom( beanInterface ) ) beanInterface = DynamicMBean.class;
        this.beanInterface = beanInterface;
    }

    protected abstract Neo4jMBean createMBean( ManagementData management )
            throws NotCompliantMBeanException;

    protected Neo4jMBean createMXBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return createMBean( management );
    }

    final Neo4jMBean loadBean( KernelData kernel, ManagementSupport support )
    {
        try
        {
            if ( support.supportsMxBeans() )
            {
                return createMXBean( new ManagementData( this, kernel, support ) );
            }
            else
            {
                return createMBean( new ManagementData( this, kernel, support ) );
            }
        }
        catch ( Exception e )
        {
            return null;
        }
    }
}
