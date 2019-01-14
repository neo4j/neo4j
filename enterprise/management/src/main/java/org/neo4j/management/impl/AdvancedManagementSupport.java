/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
        return true;
    }

    @Override
    protected final <T> T makeProxy( KernelBean kernel, ObjectName name, Class<T> beanInterface )
    {
        return BeanProxy.load( getMBeanServer(), beanInterface, name );
    }

    @Override
    protected String getBeanName( Class<?> beanInterface )
    {
        if ( beanInterface == DynamicMBean.class )
        {
            return ConfigurationBean.CONFIGURATION_MBEAN_NAME;
        }
        return KernelProxy.beanName( beanInterface );
    }

    @Override
    protected ObjectName createObjectName( String instanceId, String beanName, boolean query, String... extraNaming )
    {
        return query ? KernelProxy.createObjectNameQuery( instanceId, beanName, extraNaming ) : KernelProxy
                .createObjectName( instanceId, beanName, extraNaming );
    }
}
