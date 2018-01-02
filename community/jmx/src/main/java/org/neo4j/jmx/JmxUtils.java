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
package org.neo4j.jmx;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;

import static java.lang.String.format;

public class JmxUtils
{
    private static final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    public static ObjectName getObjectName( GraphDatabaseService db, String name )
    {
        if(!(db instanceof GraphDatabaseAPI))
        {
            throw new IllegalArgumentException( "Can only resolve object names for embedded Neo4j database " +
                    "instances, eg. instances created by GraphDatabaseFactory or HighlyAvailableGraphDatabaseFactory." );
        }
        ObjectName neoQuery = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( Kernel.class ).getMBeanQuery();

        String instance = neoQuery.getKeyProperty( "instance" );
        String domain = neoQuery.getDomain();
        try
        {
            return new ObjectName( format( "%s:instance=%s,name=%s", domain, instance, name ) );
        }
        catch ( MalformedObjectNameException e )
        {
            throw new RuntimeException( e );
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getAttribute( ObjectName objectName, String attribute )
    {
        try
        {
            return (T) mbeanServer.getAttribute( objectName, attribute );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T invoke( ObjectName objectName, String attribute, Object[] params, String[] signatur )
    {
        try
        {
            return (T) mbeanServer.invoke( objectName, attribute, params, signatur );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
