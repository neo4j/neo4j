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

package org.neo4j.webadmin.domain;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;

import org.neo4j.rest.domain.Representation;

@SuppressWarnings( "restriction" )
public class JmxAttributeRepresentation implements Representation
{

    protected ObjectName objectName;
    protected MBeanAttributeInfo attrInfo;
    protected MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();

    public JmxAttributeRepresentation( ObjectName objectName,
            MBeanAttributeInfo attrInfo )
    {
        this.objectName = objectName;
        this.attrInfo = attrInfo;
    }

    public Object serialize()
    {
        Map<String, Object> data = new HashMap<String, Object>();

        data.put( "name", attrInfo.getName() );
        data.put( "description", attrInfo.getDescription() );
        data.put( "type", attrInfo.getType() );

        data.put( "isReadable", attrInfo.isReadable() ? "true" : "false" );
        data.put( "isWriteable", attrInfo.isWritable() ? "true" : "false" );
        data.put( "isIs", attrInfo.isIs() ? "true" : "false" );

        try
        {
            Object value = jmxServer.getAttribute( objectName,
                    attrInfo.getName() );

            if ( value == null )
            {
                data.put( "value", null );
            }
            else if ( value.getClass().isArray() )
            {
                ArrayList<Object> values = new ArrayList<Object>();

                for ( Object subValue : (Object[]) value )
                {
                    if ( subValue instanceof CompositeData )
                    {
                        values.add( ( new JmxCompositeDataRepresentation(
                                (CompositeData) subValue ) ).serialize() );
                    }
                    else
                    {
                        values.add( subValue.toString() );
                    }
                }

                data.put( "value", values );
            }
            else
            {
                data.put( "value", value.toString() );
            }

        }
        catch ( AttributeNotFoundException e )
        {
            e.printStackTrace();
            data.put( "value", "N/A" );
        }
        catch ( InstanceNotFoundException e )
        {
            e.printStackTrace();
            data.put( "value", "N/A" );
        }
        catch ( MBeanException e )
        {
            e.printStackTrace();
            data.put( "value", "N/A" );
        }
        catch ( ReflectionException e )
        {
            e.printStackTrace();
            data.put( "value", "N/A" );
        }
        catch ( RuntimeMBeanException e )
        {
            data.put( "value", "N/A" );
        }
        catch ( ClassCastException e )
        {
            e.printStackTrace();
            data.put( "value", "N/A" );
        }

        return data;

    }
}
