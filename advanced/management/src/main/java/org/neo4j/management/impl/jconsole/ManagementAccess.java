/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.management.impl.jconsole;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.neo4j.management.impl.KernelProxy;

class ManagementAccess extends KernelProxy
{
    private ManagementAccess( MBeanServerConnection server, ObjectName kernel )
    {
        super( server, kernel );
    }

    @Override
    protected <T> T getBean( Class<T> beanInterface )
    {
        return super.getBean( beanInterface );
    }

    static ManagementAccess[] getAll( MBeanServerConnection server )
    {
        final Set<ObjectName> names;
        try
        {
            names = server.queryNames( createObjectName( "*", KERNEL_BEAN_NAME ), null );
        }
        catch ( IOException e )
        {
            return new ManagementAccess[0];
        }
        ManagementAccess[] proxies = new ManagementAccess[names.size()];
        Iterator<ObjectName> iter = names.iterator();
        for ( int i = 0; i < proxies.length || iter.hasNext(); i++ )
        {
            proxies[i] = new ManagementAccess( server, iter.next() );
        }
        return proxies;
    }

    public ObjectName getMBeanQuery()
    {
        return super.mbeanQuery();
    }
}