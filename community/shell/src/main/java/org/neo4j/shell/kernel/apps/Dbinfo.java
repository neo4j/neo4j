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
package org.neo4j.shell.kernel.apps;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

public class Dbinfo extends NonTransactionProvidingApp
{
    {
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.MAY,
                "List available attributes for the specified bean. "
                        + "Including a description about each attribute." ) );
        addOptionDefinition( "g", new OptionDefinition( OptionValueType.MUST,
                "Get the value of the specified attribute(s), "
                        + "or all attributes of the specified bean "
                        + "if no attributes are specified." ) );
    }

    @Override
    public String getDescription()
    {
        final Kernel kernel;
        try
        {
            kernel = getKernel();
        }
        catch ( ShellException e )
        {
            return e.getMessage();
        }
        MBeanServer mbeans = getPlatformMBeanServer();
        StringBuilder result = new StringBuilder(
                "Get runtime information about the Graph Database.\n"
                        + "This uses the Neo4j management beans to get"
                        + " information about the Graph Database.\n\n" );
        availableBeans( mbeans, kernel, result );
        result.append( "\n" );
        getUsage( result );
        return result.toString();
    }

    private void getUsage( StringBuilder result )
    {
        result.append( "USAGE: " );
        result.append( getName() );
        result.append( " -(g|l) <bean name> [list of attribute names]" );
    }

    private Kernel getKernel() throws ShellException
    {
        GraphDatabaseAPI graphDb = getServer().getDb();
        Kernel kernel = null;
        if ( graphDb instanceof GraphDatabaseAPI )
        {
            try
            {
                kernel = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class ).getSingleManagementBean( Kernel.class );
            }
            catch ( Exception e )
            {
                // Ignore - the null check does the work
            }
        }
        if ( kernel == null )
        {
            throw new ShellException( getName() + " is not available for this graph database." );
        }
        return kernel;
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        Kernel kernel = getKernel();
        boolean list = parser.options().containsKey( "l" ), get = parser.options().containsKey( "g" );
        if ( (list && get) || (!list && !get) )
        {
            StringBuilder usage = new StringBuilder();
            getUsage( usage );
            usage.append( ".\n" );
            out.print( usage.toString() );
            return Continuation.INPUT_COMPLETE;
        }
        MBeanServer mbeans = getPlatformMBeanServer();
        String bean = null;
        String[] attributes = null;
        if ( list )
        {
            bean = parser.options().get( "l" );
        }
        else if ( get )
        {
            bean = parser.options().get( "g" );
            attributes = parser.arguments().toArray( new String[parser.arguments().size()] );
        }
        if ( bean == null ) // list beans
        {
            StringBuilder result = new StringBuilder();
            availableBeans( mbeans, kernel, result );
            out.print( result.toString() );
            return Continuation.INPUT_COMPLETE;
        }
        ObjectName mbean;
        {
            mbean = kernel.getMBeanQuery();
            Hashtable<String, String> properties = new Hashtable<String, String>(
                    mbean.getKeyPropertyList() );
            properties.put( "name", bean );
            try
            {
                Iterator<ObjectName> names = mbeans.queryNames(
                        new ObjectName( mbean.getDomain(), properties ), null ).iterator();
                if ( names.hasNext() )
                {
                    mbean = names.next();
                    if ( names.hasNext() )
                    {
                        mbean = null;
                    }
                }
                else
                {
                    mbean = null;
                }
            }
            catch ( Exception e )
            {
                mbean = null;
            }
        }
        if ( mbean == null )
        {
            throw new ShellException( "No such management bean \"" + bean + "\"." );
        }
        if ( attributes == null ) // list attributes
        {
            for ( MBeanAttributeInfo attr : mbeans.getMBeanInfo( mbean ).getAttributes() )
            {
                out.println( attr.getName() + " - " + attr.getDescription() );
            }
        }
        else
        {
            if ( attributes.length == 0 ) // specify all attributes
            {
                MBeanAttributeInfo[] allAttributes = mbeans.getMBeanInfo( mbean ).getAttributes();
                attributes = new String[allAttributes.length];
                for ( int i = 0; i < allAttributes.length; i++ )
                {
                    attributes[i] = allAttributes[i].getName();
                }
            }
            JSONObject json = new JSONObject();
            for ( Object value : mbeans.getAttributes( mbean, attributes ) )
            {
                printAttribute( json, value );
            }
            out.println( json.toString( 2 ) );
        }
        return Continuation.INPUT_COMPLETE;
    }

    private void printAttribute( JSONObject json, Object value ) throws RemoteException, ShellException
    {
        try
        {
            Attribute attribute = (Attribute) value;
            Object attributeValue = attribute.getValue();
            if ( attributeValue != null && attributeValue.getClass().isArray() )
            {
                Object[] arrayValue = (Object[]) attributeValue;
                JSONArray array = new JSONArray();
                for ( Object item : (Object[]) arrayValue )
                {
                    if ( item instanceof CompositeData )
                    {
                        array.put( compositeDataAsMap( (CompositeData) item ) );
                    }
                    else
                    {
                        array.put( item.toString() );
                    }
                }
                json.put( attribute.getName(), array );
            }
            else
            {
                json.put( attribute.getName(), attributeValue );
            }
        }
        catch ( JSONException e )
        {
            throw ShellException.wrapCause( e );
        }
    }

    private Map<?, ?> compositeDataAsMap( CompositeData item )
    {
        Map<String, Object> result = new HashMap<String, Object>();
        CompositeData compositeData = (CompositeData) item;
        for ( String key : compositeData.getCompositeType().keySet() )
        {
            result.put( key, compositeData.get( key ) );
        }
        return result;
    }

    private void availableBeans( MBeanServer mbeans, Kernel kernel, StringBuilder result )
    {
        result.append( "Available Management Beans\n" );
        for ( Object name : mbeans.queryNames( kernel.getMBeanQuery(), null ) )
        {
            result.append( "* " );
            result.append( ((ObjectName) name).getKeyProperty( "name" ) );
            result.append( "\n" );
        }
    }
}
