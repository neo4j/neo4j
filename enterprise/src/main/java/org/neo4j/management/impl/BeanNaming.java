/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

class BeanNaming
{
    private BeanNaming()
    {
    }

    static ObjectName getObjectName( String instanceId, Class<?> beanInterface, String beanName )
    {
        final String name = beanName( beanInterface, beanName );
        if ( name == null ) return null;
        StringBuilder identifier = new StringBuilder( "org.neo4j:" );
        identifier.append( "instance=kernel#" );
        identifier.append( instanceId == null ? "*" : instanceId );
        identifier.append( ",name=" );
        identifier.append( name );
        try
        {
            return new ObjectName( identifier.toString() );
        }
        catch ( MalformedObjectNameException e )
        {
            return null;
        }
    }

    public static ObjectName getObjectName( ObjectName beanQuery, Class<?> beanInterface,
            String beanName )
    {
        String name = beanName( beanInterface, beanName );
        if ( name == null ) return null;
        Hashtable<String, String> properties = new Hashtable<String, String>(
                beanQuery.getKeyPropertyList() );
        properties.put( "name", name );
        try
        {
            return new ObjectName( beanQuery.getDomain(), properties );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Could not create specified MBean Query." );
        }
    }

    private static String beanName( Class<?> beanInterface, String beanName )
    {
        final String name;
        if ( beanName != null )
        {
            name = beanName;
        }
        else if ( beanInterface == null )
        {
            name = "*";
        }
        else
        {
            try
            {
                name = (String) beanInterface.getField( "NAME" ).get( null );
            }
            catch ( Exception e )
            {
                return null;
            }
        }
        return name;
    }
}
