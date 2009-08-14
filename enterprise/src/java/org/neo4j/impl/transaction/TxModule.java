/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

import java.io.IOException;
import java.util.Map;

import javax.transaction.TransactionManager;
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
 * Can reads a XA data source configuration file and registers all the data
 * sources defined there or be used to manually add XA data sources.
 * <p>
 * This module will create a instance of each {@link XaDataSource} once started
 * and will close them once stopped.
 * 
 * @see XaDataSourceManager
 */
public class TxModule
{
    private static final String MODULE_NAME = "TxModule";

    private boolean startIsOk = true;
    private String dataSourceConfigFile = null;
    private String txLogDir = "var/tm";

    private final TxManager txManager;
    private final XaDataSourceManager xaDsManager;

    public TxModule( String txLogDir )
    {
        this.txLogDir = txLogDir;
        this.txManager = new TxManager( txLogDir );
        this.xaDsManager = new XaDataSourceManager();
    }

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
        txManager.init( xaDsManager );
        startIsOk = false;
    }

    /**
     * Sets a XA data source configuration file.
     * 
     * @param fileName
     *            The filename of the configuration file
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
        xaDsManager.unregisterAllDataSources();
        txManager.stop();
    }

    public void destroy()
    {
    }

    public String getModuleName()
    {
        return MODULE_NAME;
    }

    private class XaDataSourceConfigFileParser
    {
        void parse( String file )
        {
            try
            {
                DocumentBuilder builder = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder();
                Document document = builder.parse( file );

                Element root = (Element) document.getElementsByTagName(
                    "datasources" ).item( 0 );

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
            XaDataSourceManager xaDsMgr = xaDsManager;
            NamedNodeMap attributes = element.getAttributes();
            String name = attributes.getNamedItem( "name" ).getNodeValue();
            name = name.toLowerCase();
            if ( xaDsMgr.hasDataSource( name ) )
            {
                throw new RuntimeException( "Data source[" + name
                    + "] has already been registered" );
            }
            String fqn = attributes.getNamedItem( "class" ).getNodeValue();
            String branchId = attributes.getNamedItem( "branchid" )
                .getNodeValue();
            if ( !branchId.startsWith( "0x" ) )
            {
                throw new RuntimeException( "Unable to parse branch id["
                    + branchId + "] on " + name + "[" + fqn
                    + "], branch id should start with \"0x\""
                    + " since they are hexadecimal" );
            }
            if ( branchId.length() != 8 )
            {
                throw new RuntimeException( "Unable to parse branch id["
                    + branchId + "] on " + name + "[" + fqn
                    + "], branch id must be a 3 byte hexadecimal number" );
            }
            byte resourceId[] = getBranchId( branchId.substring( 2, branchId
                .length() ) );
            Map<String,String> params = new java.util.HashMap<String,String>();
            NodeList list = element.getElementsByTagName( "param" );
            // java.util.Iterator i = element.elementIterator( "param" );
            for ( int i = 0; i < list.getLength(); i++ )
            {
                Element param = (Element) list.item( i );
                attributes = param.getAttributes();
                String key = attributes.getNamedItem( "name" ).getNodeValue();
                String value = attributes.getNamedItem( "value" )
                    .getNodeValue();
                params.put( key, value );
            }
            try
            {
                XaDataSource dataSource = xaDsMgr.create( fqn, params );
                xaDsMgr.registerDataSource( name, dataSource, resourceId );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Could not create data source "
                    + name + "[" + fqn + "]", e );
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
     * @param name
     *            The data source name
     * @param className
     *            The (full) class name of class
     * @param resourceId
     *            The resource id identifying datasource
     * @param params
     *            The configuration map for the datasource
     * @throws LifecycleException
     */
    public XaDataSource registerDataSource( String dsName, String className,
        byte resourceId[], Map<?,?> params )
    {
        XaDataSourceManager xaDsMgr = xaDsManager;
        String name = dsName.toLowerCase();
        if ( xaDsMgr.hasDataSource( name ) )
        {
            throw new RuntimeException( "Data source[" + name
                + "] has already been registered" );
        }
        try
        {
            XaDataSource dataSource = xaDsMgr.create( className, params );
            xaDsMgr.registerDataSource( name, dataSource, resourceId );
            return dataSource;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create data source [" + name
                + "], see nested exception for cause of error", e );
        }
    }

    public XaDataSource registerDataSource( String dsName, String className,
        byte resourceId[], Map<?,?> params, boolean useExisting )
    {
        XaDataSourceManager xaDsMgr = xaDsManager;
        String name = dsName.toLowerCase();
        if ( xaDsMgr.hasDataSource( name ) )
        {
            if ( useExisting )
            {
                return xaDsMgr.getXaDataSource( name );
            }
            throw new RuntimeException( "Data source[" + name
                + "] has already been registered" );
        }
        try
        {
            XaDataSource dataSource = xaDsMgr.create( className, params );
            xaDsMgr.registerDataSource( name, dataSource, resourceId );
            return dataSource;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create data source " + name
                + "[" + name + "]", e );
        }
    }

    public String getTxLogDirectory()
    {
        return txLogDir;
    }

    public TransactionManager getTxManager()
    {
        return txManager;
    }
    
    public XaDataSourceManager getXaDataSourceManager()
    {
        return xaDsManager;
    }
}