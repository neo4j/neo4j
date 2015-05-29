/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
    private final DependencyResolver dependencies;
    private final KernelData kernel;
    private final ManagementSupport support;
    final ManagementBeanProvider provider;

    ManagementData( ManagementBeanProvider provider, DependencyResolver dependencies, KernelData kernel,
            ManagementSupport support )
    {
        this.provider = provider;
        this.dependencies = dependencies;
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

    // TODO: This is an artifact of Management Beans getting loaded via service loader pattern, rather than
    // assembled programatically. We should refactor this such that each EditionModule assembles the beans
    // it wants to expose and uses DI to provide whatever services each bean needs. That way we don't
    // need management bean constructors to change their behavior based on which GDS interface they detect
    public boolean isHAMode()
    {
        return kernel.isHAMode();
    }

    @Override
    public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
    {
        // TODO: Every call to this method is an indication that something is missing dependency injection.
        // Mainly this is from ManagementBeans, which are created via Service loading, which needs to be changed
        // before we can remove this
        return dependencies.resolveDependency( type, selector );
    }
}
