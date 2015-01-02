/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.core.NoTransactionState;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.DummyXaDataSource.DummyXaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;

import static org.mockito.Mockito.mock;

public class TestXaFramework extends AbstractNeo4jTestCase
{
    private TransactionManager tm;
    private XaDataSourceManager xaDsMgr;
    private final TransactionStateFactory stateFactory = new TransactionStateFactory( new DevNullLoggingService() )
    {
        @Override
        public TransactionState create( Transaction tx )
        {
            return new NoTransactionState()
            {
                @Override
                @SuppressWarnings("deprecation")
                public TxIdGenerator getTxIdGenerator()
                {
                    return TxIdGenerator.DEFAULT;
                }
            };
        }
    };

    private File path()
    {
        String path = getStorePath( "xafrmwrk" );
        File file = new File( path );
        try
        {
            FileUtils.deleteRecursively( file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        assertTrue( "create directory: " + file, file.mkdirs() );
        return file;
    }

    private File file( String name )
    {
        return new File( path(), name);
    }

    private File resourceFile()
    {
        return file( "dummy_resource" );
    }

    @Before
    public void setUpFramework()
    {
        getTransaction().finish();
        tm = getGraphDbAPI().getDependencyResolver().resolveDependency( TransactionManager.class );
        xaDsMgr = getGraphDbAPI().getDependencyResolver().resolveDependency( XaDataSourceManager.class );
    }

    @Test
    public void testCreateXaResource() throws Exception
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put( "store_dir", "target/var" );
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        KernelHealth kernelHealth = mock( KernelHealth.class );
        xaDsMgr.registerDataSource( new DummyXaDataSource(
                UTF8.encode( "DDDDDD" ), "dummy_datasource",
                new XaFactory(
                        new Config( config, GraphDatabaseSettings.class ), TxIdGenerator.DEFAULT,
                        new PlaceboTm( null, getGraphDbAPI().getDependencyResolver()
                                .resolveDependency( TxIdGenerator.class ) ),
                        fileSystem, new Monitors(), new DevNullLoggingService(),
                        RecoveryVerifier.ALWAYS_VALID, LogPruneStrategies.NO_PRUNING, kernelHealth ), stateFactory,
                        resourceFile() ) );
        XaDataSource xaDs = xaDsMgr.getXaDataSource( "dummy_datasource" );
        DummyXaConnection xaC = null;
        try
        {
            xaC = (DummyXaConnection) xaDs.getXaConnection();
            try
            {
                xaC.doStuff1();
                fail( "Non enlisted resource should throw exception" );
            }
            catch ( XAException e )
            { // good
            }
            Xid xid = new XidImpl( new byte[0], new byte[0] );
            xaC.getXaResource().start( xid, XAResource.TMNOFLAGS );
            try
            {
                xaC.doStuff1();
                xaC.doStuff2();
            }
            catch ( XAException e )
            {
                fail( "Enlisted resource should not throw exception" );
            }
            xaC.getXaResource().end( xid, XAResource.TMSUCCESS );
            xaC.getXaResource().prepare( xid );
            xaC.getXaResource().commit( xid, false );
        }
        finally
        {

            xaDsMgr.unregisterDataSource( "dummy_datasource" );
            if ( xaC != null )
            {
                xaC.destroy();
            }
        }
        // cleanup dummy resource log
        deleteAllResourceFiles();
    }

    @Test
    public void testTxIdGeneration() throws Exception
    {
        DummyXaConnection xaC1 = null;
        try
        {
            Map<String, String> config = new HashMap<String, String>();
            config.put( "store_dir", "target/var" );
            FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            KernelHealth kernelHealth = mock( KernelHealth.class );

            xaDsMgr.registerDataSource( new DummyXaDataSource( UTF8.encode( "DDDDDD" ), "dummy_datasource1",
                    new XaFactory( new Config( config, GraphDatabaseSettings.class ), TxIdGenerator.DEFAULT,
                            (AbstractTransactionManager)tm, fileSystem, new Monitors(), new DevNullLoggingService(),
                            RecoveryVerifier.ALWAYS_VALID, LogPruneStrategies.NO_PRUNING, kernelHealth ),
                            stateFactory, resourceFile() ) );
            DummyXaDataSource xaDs1 = (DummyXaDataSource) xaDsMgr.getXaDataSource( "dummy_datasource1" );
            xaC1 = (DummyXaConnection) xaDs1.getXaConnection();
            tm.begin(); // get
            xaC1.enlistWithTx( tm );
            int currentTxId = xaC1.getTransactionId();
            xaC1.doStuff1();
            xaC1.delistFromTx( tm );
            tm.commit();
            // xaC2 = ( DummyXaConnection ) xaDs2.getXaConnection();
            tm.begin();
            Node node = getGraphDb().createNode(); // get resource in tx
            xaC1.enlistWithTx( tm );
            assertEquals( ++currentTxId, xaC1.getTransactionId() );
            xaC1.doStuff1();
            xaC1.delistFromTx( tm );
            tm.commit();
            tm.begin();
            node = getGraphDb().getNodeById( node.getId() );
            xaC1.enlistWithTx( tm );
            assertEquals( ++currentTxId, xaC1.getTransactionId() );
            xaC1.doStuff2();
            xaC1.delistFromTx( tm );
            node.delete();
            tm.commit();
        }
        finally
        {
            xaDsMgr.unregisterDataSource( "dummy_datasource1" );
            // xaDsMgr.unregisterDataSource( "dummy_datasource1" );
            if ( xaC1 != null )
            {
                xaC1.destroy();
            }
        }
        // cleanup dummy resource log
        deleteAllResourceFiles();
    }

    private void deleteAllResourceFiles()
    {
        File dir = new File( "." );
        final String prefix = resourceFile().getPath();
        File files[] = dir.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String fileName )
            {
                return fileName.startsWith( prefix );
            }
        } );
        boolean allDeleted = true;
        for ( File file : files )
        {
            if ( !file.delete() )
            {
                allDeleted = false;
            }
        }
        assertTrue( "delete all files starting with " + prefix, allDeleted );
    }
}
