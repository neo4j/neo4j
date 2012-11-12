/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.util.Map;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

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
    private String txLogDir = "var/tm";

    private final AbstractTransactionManager txManager;
    private final XaDataSourceManager xaDsManager;
    private final KernelPanicEventGenerator kpe;

    private final TxHook txHook;

    public TxModule( String txLogDir, KernelPanicEventGenerator kpe, TxHook txHook, StringLogger msgLog, FileSystemAbstraction fileSystem,
            String serviceName )
    {
        this.txLogDir = txLogDir;
        this.kpe = kpe;
        this.txHook = txHook;
        TransactionManagerProvider provider;
        if ( serviceName == null )
        {
            provider = new DefaultTransactionManagerProvider();
        }
        else {
            provider = Service.load( TransactionManagerProvider.class, serviceName );
            if ( provider == null )
            {
                throw new IllegalStateException( "Unknown transaction manager implementation: "
                                                 + serviceName );
            }
        }
        txManager = provider.loadTransactionManager( txLogDir, kpe, txHook, msgLog, fileSystem );
        this.xaDsManager = new XaDataSourceManager();
    }

    public TxModule( boolean readOnly, KernelPanicEventGenerator kpe )
    {
        this.kpe = kpe;
        if ( readOnly )
        {
            this.txManager = new ReadOnlyTxManager();
            this.xaDsManager = new XaDataSourceManager();
            this.txHook = CommonFactories.defaultTxHook();
        }
        else
        {
            throw new IllegalStateException( "Read only must be set for this constructor" );
        }
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
        txManager.init( xaDsManager );
        startIsOk = false;
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
            throw new TransactionFailureException( "Data source[" + name
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
            throw new TransactionFailureException(
                "Could not create data source [" + name
                + "], see nested exception for cause of error", e.getCause() );
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
            throw new TransactionFailureException( "Data source[" + name
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
            throw new TransactionFailureException(
                "Could not create data source " + name + "[" + name + "]", e );
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
    
    public TxHook getTxHook()
    {
        return txHook;
    }

    public int getStartedTxCount()
    {
        if ( txManager instanceof TxManager )
        {
            return ((TxManager) txManager).getStartedTxCount();
        }
        return 0;
    }

    public int getCommittedTxCount()
    {
        if ( txManager instanceof TxManager )
        {
            return ((TxManager) txManager).getCommittedTxCount();
        }
        return 0;
    }

    public int getRolledbackTxCount()
    {
        if ( txManager instanceof TxManager )
        {
            return ((TxManager) txManager).getRolledbackTxCount();
        }
        return 0;
    }

    public int getActiveTxCount()
    {
        if ( txManager instanceof TxManager )
        {
            return ((TxManager) txManager).getActiveTxCount();
        }
        return 0;
    }

    public int getPeakConcurrentTxCount()
    {
        if ( txManager instanceof TxManager )
        {
            return ((TxManager) txManager).getPeakConcurrentTxCount();
        }
        return 0;
    }
}