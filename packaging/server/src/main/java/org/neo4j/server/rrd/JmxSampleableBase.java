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
package org.neo4j.server.rrd;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.management.Kernel;
import org.neo4j.management.Primitives;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.lang.management.ManagementFactory;

public abstract class JmxSampleableBase  implements Sampleable
{
    private MBeanServer mbeanServer;
    private ObjectName objectName;

    public JmxSampleableBase( AbstractGraphDatabase graphDb ) throws MalformedObjectNameException
    {
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName neoQuery = graphDb.getManagementBean( Kernel.class ).getMBeanQuery();
        String instance = neoQuery.getKeyProperty( "instance" );
        String baseName = neoQuery.getDomain() + ":instance=" + instance + ",name=";
        objectName = new ObjectName( baseName + Primitives.NAME );
    }

    public abstract String getName();

    public long getValue()
    {
        try
        {
            return (Long)mbeanServer.getAttribute( objectName, getJmxAttributeName() );
        } catch ( MBeanException e )
        {
            throw new RuntimeException( e );
        } catch ( AttributeNotFoundException e )
        {
            throw new RuntimeException( e );
        } catch ( InstanceNotFoundException e )
        {
            throw new RuntimeException( e );
        } catch ( ReflectionException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected abstract String getJmxAttributeName();
}
