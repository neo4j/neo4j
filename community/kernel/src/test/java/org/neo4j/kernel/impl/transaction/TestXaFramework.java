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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.transaction.TransactionStateFactory.NO_STATE_FACTORY;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

public class TestXaFramework extends AbstractNeo4jTestCase
{
    private TransactionManager tm;
    private XaDataSourceManager xaDsMgr;

    private String path()
    {
        String path = getStorePath( "xafrmwrk" );
        try
        {
            FileUtils.deleteRecursively( new File( path ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        new File( path ).mkdirs();
        return path;
    }

    private String file( String name )
    {
        return path() + File.separator + name;
    }

    private String resourceFile()
    {
        return file( "dummy_resource" );
    }

    @Before
    public void setUpFramework()
    {
        getTransaction().finish();
        tm = getEmbeddedGraphDb().getTxManager();
        xaDsMgr = getEmbeddedGraphDb().getXaDataSourceManager();
    }

    private static class DummyCommand extends XaCommand
    {
        private int type = -1;

        DummyCommand( int type )
        {
            this.type = type;
        }

        @Override
        public void execute()
        {
        }

        // public void writeToFile( FileChannel fileChannel, ByteBuffer buffer )
        // throws IOException
        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            // buffer.clear();
            buffer.putInt( type );
            // buffer.flip();
            // fileChannel.write( buffer );
        }
    }

    private static class DummyCommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
        {
            buffer.clear();
            buffer.limit( 4 );
            if ( byteChannel.read( buffer ) == 4 )
            {
                buffer.flip();
                return new DummyCommand( buffer.getInt() );
            }
            return null;
        }
    }

    private static class DummyTransaction extends XaTransaction
    {
        private java.util.List<XaCommand> commandList = new java.util.ArrayList<XaCommand>();

        public DummyTransaction( int identifier, XaLogicalLog log )
        {
            super( identifier, log );
            setCommitTxId( 0 );
        }

        @Override
        public void doAddCommand( XaCommand command )
        {
            commandList.add( command );
        }

//        public XaCommand[] getCommands()
//        {
//            return commandList.toArray( new XaCommand[commandList.size()] );
//        }

        @Override
        public void doPrepare()
        {

        }

        @Override
        public void doRollback()
        {
        }

        @Override
        public void doCommit()
        {
        }

        @Override
        public boolean isReadOnly()
        {
            return false;
        }
    }

    private static class DummyTransactionFactory extends XaTransactionFactory
    {
        @Override
        public XaTransaction create( int identifier, TransactionState state )
        {
            return new DummyTransaction( identifier, getLogicalLog() );
        }

        @Override
        public void flushAll()
        {
        }

        @Override
        public long getAndSetNewVersion()
        {
            return 0;
        }

        @Override
        public long getCurrentVersion()
        {
            return 0;
        }
        
        @Override
        public void setVersion( long version )
        {
        }

        @Override
        public long getLastCommittedTx()
        {
            return 0;
        }
    }

    public class DummyXaDataSource extends XaDataSource
    {
        private XaContainer xaContainer = null;

        public DummyXaDataSource( java.util.Map<String,String> map, byte[] branchId, String name, XaFactory xaFactory)
            throws InstantiationException
        {
            super( branchId, name );
            try
            {
                map.put( "store_dir", path() );
                xaContainer = xaFactory.newXaContainer( this, resourceFile(),
                        new DummyCommandFactory(),
                        new DummyTransactionFactory(), NO_STATE_FACTORY, new TransactionInterceptorProviders(
                        Iterables.<TransactionInterceptorProvider>empty(),
                        new DependencyResolver()
                        {
                            @Override
                            public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
                            {
                                return (T) new Config( MapUtil.stringMap(
                                        GraphDatabaseSettings.intercept_committing_transactions.name(),
                                        GraphDatabaseSetting.FALSE,
                                        GraphDatabaseSettings.intercept_deserialized_transactions.name(),
                                        GraphDatabaseSetting.FALSE
                                        ));
                            }
                        }));
                xaContainer.openLogicalLog();
            }
            catch ( IOException e )
            {
                throw new InstantiationException( "" + e );
            }
        }

        public void init()
        {
        }

        @Override
        public void start()
        {
        }

        @Override
        public void stop()
        {
            xaContainer.close();
            // cleanup dummy resource log
            File dir = new File( "." );
            File files[] = dir.listFiles( new FilenameFilter()
            {
                public boolean accept( File dir, String fileName )
                {
                    return fileName.startsWith( resourceFile() );
                }
            } );
            for ( int i = 0; i < files.length; i++ )
            {
                files[i].delete();
            }
        }

        @Override
        public void shutdown()
        {
        }


        @Override
        public XaConnection getXaConnection()
        {
            return new DummyXaConnection( xaContainer.getResourceManager() );
        }

        @Override
        public long getLastCommittedTxId()
        {
            return 0;
        }
    }

    private static class DummyXaResource extends XaResourceHelpImpl
    {
        DummyXaResource( XaResourceManager xaRm )
        {
            super( xaRm, null );
        }

        @Override
        public boolean isSameRM( XAResource resource )
        {
            if ( resource instanceof DummyXaResource )
            {
                return true;
            }
            return false;
        }
    }

    private class DummyXaConnection extends XaConnectionHelpImpl
    {
        private XAResource xaResource = null;

        public DummyXaConnection( XaResourceManager xaRm )
        {
            super( xaRm );
            xaResource = new DummyXaResource( xaRm );
        }

        @Override
        public XAResource getXaResource()
        {
            return xaResource;
        }

        public void doStuff1() throws XAException
        {
            validate();
            getTransaction().addCommand( new DummyCommand( 1 ) );
        }

        public void doStuff2() throws XAException
        {
            validate();
            getTransaction().addCommand( new DummyCommand( 2 ) );
        }

        public void enlistWithTx() throws Exception
        {
            tm.getTransaction().enlistResource( xaResource );
        }

        public void delistFromTx() throws Exception
        {
            tm.getTransaction().delistResource( xaResource,
                XAResource.TMSUCCESS );
        }

        public int getTransactionId() throws Exception
        {
            return getTransaction().getIdentifier();
        }
    }

    @Test
    public void testCreateXaResource() throws Exception
    {
        Map<String,String> config = new HashMap<String,String>();
        config.put( "store_dir", "target/var" );
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        xaDsMgr.registerDataSource( new DummyXaDataSource(
                config, UTF8.encode( "DDDDDD" ), "dummy_datasource",
                new XaFactory(new Config( new ConfigurationDefaults(GraphDatabaseSettings.class ).apply( config )),
                        TxIdGenerator.DEFAULT, new PlaceboTm(), new DefaultLogBufferFactory(),
                        fileSystem, StringLogger.DEV_NULL,
                        RecoveryVerifier.ALWAYS_VALID, LogPruneStrategies.NO_PRUNING )) );
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
        File dir = new File( "." );
        File files[] = dir.listFiles( new FilenameFilter()
        {
            public boolean accept( File dir, String fileName )
            {
                return fileName.startsWith( resourceFile() );
            }
        } );
        for ( int i = 0; i < files.length; i++ )
        {
            files[i].delete();
        }
    }

    @Test
    public void testTxIdGeneration() throws Exception
    {
        DummyXaDataSource xaDs1 = null;
        DummyXaConnection xaC1 = null;
        try
        {
            Map<String,String> config = new HashMap<String,String>();
            config.put( "store_dir", "target/var" );
            FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            xaDsMgr.registerDataSource(new DummyXaDataSource( config, UTF8.encode( "DDDDDD" ),"dummy_datasource1" , new XaFactory(new Config( new ConfigurationDefaults(GraphDatabaseSettings.class ).apply( config )), TxIdGenerator.DEFAULT, new PlaceboTm(), new DefaultLogBufferFactory(), fileSystem, StringLogger.DEV_NULL, RecoveryVerifier.ALWAYS_VALID, LogPruneStrategies.NO_PRUNING )) );
            xaDs1 = (DummyXaDataSource) xaDsMgr
                .getXaDataSource( "dummy_datasource1" );
            xaC1 = (DummyXaConnection) xaDs1.getXaConnection();
            tm.begin(); // get
            xaC1.enlistWithTx();
            int currentTxId = xaC1.getTransactionId();
            xaC1.doStuff1();
            xaC1.delistFromTx();
            tm.commit();
            // xaC2 = ( DummyXaConnection ) xaDs2.getXaConnection();
            tm.begin();
            Node node = getGraphDb().createNode(); // get resource in tx
            xaC1.enlistWithTx();
            assertEquals( ++currentTxId, xaC1.getTransactionId() );
            xaC1.doStuff1();
            xaC1.delistFromTx();
            tm.commit();
            tm.begin();
            node = getGraphDb().getNodeById( node.getId() );
            xaC1.enlistWithTx();
            assertEquals( ++currentTxId, xaC1.getTransactionId() );
            xaC1.doStuff2();
            xaC1.delistFromTx();
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
        File dir = new File( "." );
        File files[] = dir.listFiles( new FilenameFilter()
        {
            public boolean accept( File dir, String fileName )
            {
                return fileName.startsWith( resourceFile() );
            }
        } );
        for ( int i = 0; i < files.length; i++ )
        {
            files[i].delete();
        }
    }
}
