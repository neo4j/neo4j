package org.neo4j.impl.transaction;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.neo4j.impl.transaction.xaframework.XaDataSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Can reads a XA data source configuration file and registers all the 
 * data sources defiend there or be used to manually add XA data sources.
 * <p>
 * This module will create a instance of each {@link XaDataSource} once 
 * started and will close them once stopped.
 *
 * @see XaDataSourceManager
 */
public class TxModule
{
	private static final String	MODULE_NAME	= "TxModule";
	
	private boolean startIsOk = true;
	private String dataSourceConfigFile = null;
	private String txLogDir = "var/tm";

	public void init()
	{
	}
	
	public void start()
	{
		if ( !startIsOk )
		{
			return;
		}
		if ( dataSourceConfigFile != null )
		{
			new XaDataSourceConfigFileParser().parse( dataSourceConfigFile );
		}
		startIsOk = false;
	}
	
	/**
	 * Sets a XA data source configuration file.
	 * 
	 * @param fileName The filename of the configuration file
	 */
	public void setXaDataSourceConfig( String fileName )
	{
		this.dataSourceConfigFile = fileName;
	}
	
	public String getXaDataSourceConfig()
	{
		return dataSourceConfigFile;
	}

	public void reload()
	{
		stop();
		start();
	}
	
	public void stop()
	{
		XaDataSourceManager.getManager().unregisterAllDataSources();
	}
	
	public void destroy()
	{
	}
	
	public String getModuleName()
	{
		return MODULE_NAME;
	}
	
	private static class XaDataSourceConfigFileParser
	{
		void parse( String file )
		{
			try
			{
				DocumentBuilder builder = 
					DocumentBuilderFactory.newInstance().newDocumentBuilder();
				Document document = builder.parse( file );

				Element root = (Element)
					document.getElementsByTagName( "datasources" ).item( 0 );

				NodeList list = root.getElementsByTagName( "xadatasource" );
				for ( int i = 0; i < list.getLength(); i++ )
				{
					this.parseXaDataSourceElement( (Element) list.item( i ) );
				}
			}
			catch ( ParserConfigurationException e )
			{
				throw new RuntimeException( "Error parsing " + file, e );
			}
			catch ( SAXException e )
			{
				throw new RuntimeException( "Error parsing " + file, e );
			}
			catch ( IOException e )
			{
				throw new RuntimeException( "Error parsing " + file, e );
			}
		}

		private void parseXaDataSourceElement( Element element )
		{
			XaDataSourceManager xaDsMgr = XaDataSourceManager.getManager();
			NamedNodeMap attributes = element.getAttributes();
			String name	= attributes.getNamedItem( "name" ).getNodeValue();
			name = name.toLowerCase();
			if ( xaDsMgr.hasDataSource( name ) )
			{
				throw new RuntimeException( "Data source[" + name + 
					"] has already been registered" );
			}
			String fqn = attributes.getNamedItem( "class" ).getNodeValue();
			String branchId = 
				attributes.getNamedItem( "branchid" ).getNodeValue();
			if ( !branchId.startsWith( "0x" ) )
			{
				throw new RuntimeException( "Unable to parse branch id[" + 
					branchId + "] on " + name + "[" + fqn + 
					"], branch id should start with \"0x\"" +
					" since they are hexadecimal" );
			}
			if ( branchId.length() != 8 )
			{
				throw new RuntimeException( "Unable to parse branch id[" + 
					branchId + "] on " + name + "[" + fqn + 
					"], branch id must be a 3 byte hexadecimal number" );
			}
			byte resourceId[] = getBranchId( branchId.substring( 2, 
				branchId.length() ) );
			Map<String,String> params = 
				new java.util.HashMap<String,String>();
			NodeList list = element.getElementsByTagName( "param" );
			// java.util.Iterator i = element.elementIterator( "param" );
			for ( int i = 0; i < list.getLength(); i++ )
			{
				Element param = ( Element ) list.item( i );
				attributes = param.getAttributes();
				String key = attributes.getNamedItem( "name" ).getNodeValue();
				String value = 
					attributes.getNamedItem( "value" ).getNodeValue();
				params.put( key, value );
			}
			try
			{
				XaDataSource dataSource = xaDsMgr.create( fqn, params );
				xaDsMgr.registerDataSource( name, dataSource, resourceId );
			}
			catch ( Exception e )
			{
				throw new RuntimeException( "Could not create data source " + 
					name + "[" + fqn + "]", e );
			}
		}
		
		private byte[] getBranchId( String branchId )
		{
			byte resourceId[] = branchId.getBytes();
			return resourceId;
		}
	}
	
	/** 
	 * Use this method to add data source that can participate in transactions
	 * if you don't want a data source configuration file.
	 * 
	 * @param name The data source name
	 * @param className The (full) class name of class
	 * @param resourceId The resource id identifying datasource 
	 * @param params The configuration map for the datasource
	 * @throws LifecycleException
	 */
	public XaDataSource registerDataSource( String name, String className, 
		byte resourceId[], Map<String,String> params )
	{
		XaDataSourceManager xaDsMgr = XaDataSourceManager.getManager();
		name = name.toLowerCase();
		if ( xaDsMgr.hasDataSource( name ) )
		{
			throw new RuntimeException( "Data source[" + name + 
				"] has already been registered" );
		}
		try
		{
			XaDataSource dataSource = xaDsMgr.create( className, params );
			xaDsMgr.registerDataSource( name, dataSource, resourceId );
			return dataSource;
		}
		catch ( Exception e )
		{
			throw new RuntimeException( "Could not create data source " + 
			name + "[" + name + "]", e );
		}
	}

	public String getTxLogDirectory()
	{
		return txLogDir;
	}

	public void setTxLogDirectory( String dir )
	{
		txLogDir = dir;
	}
}
