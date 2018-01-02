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

import javax.management.ObjectName;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.KernelData;

public final class ManagementData extends DependencyResolver.Adapter
{
    private final KernelData kernel;
    private final ManagementSupport support;
    final ManagementBeanProvider provider;

    ManagementData( ManagementBeanProvider provider, KernelData kernel, ManagementSupport support )
    {
        this.provider = provider;
        this.kernel = kernel;
        this.support = support;
    }

    public KernelData getKernelData()
    {
        return kernel;
    }

    ObjectName getObjectName( String... extraNaming )
    {
        ObjectName name = support.createObjectName( kernel.instanceId(), provider.beanInterface, extraNaming );
        if ( name == null )
        {
            throw new IllegalArgumentException( provider.beanInterface
                                                + " is not a Neo4j Management Bean interface" );
        }
        return name;
    }

    void validate( Class<? extends Neo4jMBean> implClass )
    {
        if ( !provider.beanInterface.isAssignableFrom( implClass ) )
        {
            throw new IllegalStateException( implClass + " does not implement " + provider.beanInterface );
        }
    }

    @Override
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
    {
        return getKernelData().graphDatabase().getDependencyResolver().resolveDependency( type, selector );
    }
}
