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
package org.neo4j.management.impl;

import javax.management.DynamicMBean;
import javax.management.ObjectName;

import org.neo4j.jmx.impl.ConfigurationBean;
import org.neo4j.jmx.impl.KernelBean;
import org.neo4j.jmx.impl.ManagementSupport;

abstract class AdvancedManagementSupport extends ManagementSupport
{
    @Override
    protected final boolean supportsMxBeans()
    {
        return BeanProxy.supportsMxBeans();
    }

    @Override
    protected final <T> T makeProxy( KernelBean kernel, ObjectName name, Class<T> beanInterface )
    {
        return BeanProxy.load( getMBeanServer(), beanInterface, name );
    }

    @Override
    protected String getBeanName( Class<?> beanInterface )
    {
        if ( beanInterface == DynamicMBean.class ) return ConfigurationBean.CONFIGURATION_MBEAN_NAME;
        return KernelProxy.beanName( beanInterface );
    }

    @Override
    protected ObjectName createObjectName( String instanceId, String beanName, boolean query, String... extraNaming )
    {
        return query ? KernelProxy.createObjectNameQuery( instanceId, beanName, extraNaming ) : KernelProxy
                .createObjectName( instanceId, beanName, extraNaming );
    }
}
