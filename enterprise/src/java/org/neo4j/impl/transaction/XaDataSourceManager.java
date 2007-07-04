package org.neo4j.impl.transaction;


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 * All datasources that have been defined in the XA data source configuration 
 * file or manually added will be created and registered here. A mapping 
 * between "name", "data source" and "branch id" is keept by this manager.
 * <p>
 * Use the {@link #getXaDataSource} to obtain the instance of a datasource 
 * that has been defined in the XA data source configuration.
 *
 * @see XaDataSource
 */
public class XaDataSourceManager
{
	private static XaDataSourceManager manager = new XaDataSourceManager();
	
	// key = data source name, value = data source
	private Map<String,XaDataSource> dataSources = 
		new HashMap<String,XaDataSource>();
	// key = branchId, value = data source
	private Map<String,XaDataSource> branchIdMapping = 
		new HashMap<String,XaDataSource>();
	// key = data source name, value = branchId
	private Map<String,byte[]> sourceIdMapping = 
		new HashMap<String,byte[]>();
	
	private XaDataSourceManager()
	{
	}
	
	/**
	 * Returns the single instance of this class
	 * 
	 * @return The XA data source manager
	 */
	public static XaDataSourceManager getManager()
	{
		return manager;
	}
	
	XaDataSource create( String className, Map<String,String> params ) 
		throws ClassNotFoundException, //NoSuchMethodException, 
		InstantiationException, IllegalAccessException, 
		InvocationTargetException  
	{
		Class clazz = Class.forName( className );
		Constructor[] constructors = clazz.getConstructors();
		for ( Constructor constructor : constructors )
		{
			Class[] parameters = constructor.getParameterTypes();
			if ( parameters.length == 1 && parameters[0].equals( Map.class ) )
			{
				return ( XaDataSource ) constructor.newInstance( 
					new Object[] { params } );
			}
		}
		throw new InstantiationException( "Unable to instantiate " + 
			className + ", no valid constructor found." );
	}
	
	/**
	 * Convinience method since {@link #getXaDataSource} returns null if name
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
	 * @param name the name of the data source
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
				throw new RuntimeException( 
					"Unable to check is same resource", e );
			}
		}
		throw new RuntimeException( "Unable to find mapping for XAResource[" + 
			xaResource + "]" );
	}
	
	synchronized XAResource getXaResource( byte branchId[] )
	{
		XaDataSource dataSource = branchIdMapping.get( 
			new String( branchId ) );
		if ( dataSource == null )
		{
			throw new RuntimeException( "No mapping found for branchId[0x" + 
				new String( branchId ) + "]" );
		}
		return dataSource.getXaConnection().getXaResource();
	}
}