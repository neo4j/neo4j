/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import org.neo4j.impl.transaction.xaframework.XaDataSource;
import org.neo4j.impl.transaction.xaframework.XaResource;

/**
 * All datasources that have been defined in the XA data source configuration
 * file or manually added will be created and registered here. A mapping between
 * "name", "data source" and "branch id" is kept by this manager.
 * <p>
 * Use the {@link #getXaDataSource} to obtain the instance of a datasource that
 * has been defined in the XA data source configuration.
 * 
 * @see XaDataSource
 */
public class XaDataSourceManager
{
    // key = data source name, value = data source
    private final Map<String,XaDataSource> dataSources = 
        new HashMap<String,XaDataSource>();
    // key = branchId, value = data source
    private final Map<String,XaDataSource> branchIdMapping = 
        new HashMap<String,XaDataSource>();
    // key = data source name, value = branchId
    private final Map<String,byte[]> sourceIdMapping = 
        new HashMap<String,byte[]>();

    XaDataSourceManager()
    {
    }

    XaDataSource create( String className, Map<?,?> params )
        throws ClassNotFoundException,
        InstantiationException, IllegalAccessException,
        InvocationTargetException
    {
        Class<?> clazz = Class.forName( className );
        Constructor<?>[] constructors = clazz.getConstructors();
        for ( Constructor<?> constructor : constructors )
        {
            Class<?>[] parameters = constructor.getParameterTypes();
            if ( parameters.length == 1 && parameters[0].equals( Map.class ) )
            {
                return (XaDataSource) constructor
                    .newInstance( new Object[] { params } );
            }
        }
        throw new InstantiationException( "Unable to instantiate " + className
            + ", no valid constructor found." );
    }

    /**
     * Convenience method since {@link #getXaDataSource} returns null if name
     * doesn't exist.
     * 
     * @return True if name exists
     */
    public boolean hasDataSource( String name )
    {
        return dataSources.containsKey( name );
    }

    /**
     * Returns the {@link org.neo4j.impl.transaction.xaframework.XaDataSource}
     * registered as <CODE>name</CODE>. If no data source is registered with
     * that name <CODE>null</CODE> is returned.
     * 
     * @param name
     *            the name of the data source
     */
    public XaDataSource getXaDataSource( String name )
    {
        return dataSources.get( name );
    }

    /**
     * Public for testing purpose. Do not use.
     */
    public synchronized void registerDataSource( String name,
        XaDataSource dataSource, byte branchId[] )
    {
        dataSource.setBranchId( branchId );
        dataSource.setName( name );
        dataSources.put( name, dataSource );
        branchIdMapping.put( new String( branchId ), dataSource );
        sourceIdMapping.put( name, branchId );
    }

    /**
     * Public for testing purpose. Do not use.
     */
    public synchronized void unregisterDataSource( String name )
    {
        XaDataSource dataSource = dataSources.get( name );
        byte branchId[] = getBranchId( 
            dataSource.getXaConnection().getXaResource() );
        dataSources.remove( name );
        branchIdMapping.remove( new String( branchId ) );
        sourceIdMapping.remove( name );
        dataSource.close();
    }

    synchronized void unregisterAllDataSources()
    {
        branchIdMapping.clear();
        sourceIdMapping.clear();
        Iterator<XaDataSource> itr = dataSources.values().iterator();
        while ( itr.hasNext() )
        {
            XaDataSource dataSource = itr.next();
            dataSource.close();
        }
        dataSources.clear();
    }

    synchronized byte[] getBranchId( XAResource xaResource )
    {
        if ( xaResource instanceof XaResource )
        {
            byte branchId[] = ((XaResource) xaResource).getBranchId();
            if ( branchId != null )
            {
                return branchId;
            }
        }
        Iterator<Map.Entry<String,XaDataSource>> itr = 
            dataSources.entrySet().iterator();
        while ( itr.hasNext() )
        {
            Map.Entry<String,XaDataSource> entry = itr.next();
            XaDataSource dataSource = entry.getValue();
            XAResource resource = dataSource.getXaConnection().getXaResource();
            try
            {
                if ( resource.isSameRM( xaResource ) )
                {
                    String name = entry.getKey();
                    return sourceIdMapping.get( name );
                }
            }
            catch ( XAException e )
            {
                throw new RuntimeException( "Unable to check is same resource",
                    e );
            }
        }
        throw new RuntimeException( "Unable to find mapping for XAResource["
            + xaResource + "]" );
    }

    synchronized XAResource getXaResource( byte branchId[] )
    {
        XaDataSource dataSource = branchIdMapping.get( new String( branchId ) );
        if ( dataSource == null )
        {
            throw new RuntimeException( "No mapping found for branchId[0x"
                + new String( branchId ) + "]" );
        }
        return dataSource.getXaConnection().getXaResource();
    }
    
    // not thread safe
    public Iterable<XaDataSource> getAllRegisteredDataSources()
    {
        return dataSources.values();
    }
}