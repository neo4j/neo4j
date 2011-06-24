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
package org.neo4j.server.rrd;

import java.lang.management.ManagementFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.Primitives;
import org.neo4j.server.database.Database;

public abstract class DatabasePrimitivesSampleableBase implements Sampleable
{
    private final MBeanServer mbeanServer;
    private final Database database;

    public DatabasePrimitivesSampleableBase( Database db )
    {
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        this.database = db;
    }

    public abstract String getName();

    public long getValue()
    {
        try
        {
            return (Long) mbeanServer.getAttribute( getObjectName(), getJmxAttributeName() );
        }
        catch ( UnsupportedOperationException e )
        {
            // Happens when the database has been shut down
            throw new UnableToSampleException();
        }
        catch ( InstanceNotFoundException e )
        {
            throw new UnableToSampleException();
        }
        catch ( MBeanException e )
        {
            throw new RuntimeException( e );
        }
        catch ( AttributeNotFoundException e )
        {
            throw new RuntimeException( e );
        }
        catch ( ReflectionException e )
        {
            throw new RuntimeException( e );
        }
        catch ( MalformedObjectNameException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected abstract String getJmxAttributeName();

    protected ObjectName getObjectName() throws MalformedObjectNameException, NullPointerException
    {
        ObjectName neoQuery = database.graph.getManagementBean( Kernel.class )
                .getMBeanQuery();
        String instance = neoQuery.getKeyProperty( "instance" );
        String baseName = neoQuery.getDomain() + ":instance=" + instance + ",name=";
        return new ObjectName( baseName + Primitives.NAME );
    }
}
