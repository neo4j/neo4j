/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import static org.neo4j.kernel.impl.transaction.xaframework.InjectedTransactionValidator.ALLOW_ALL;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReader;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriter;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriterFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
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

public class DummyXaDataSource extends XaDataSource
{
    private XaContainer xaContainer = null;

    public DummyXaDataSource( byte[] branchId, String name, XaFactory xaFactory,
            TransactionStateFactory stateFactory, File logFile ) throws InstantiationException
    {
        super( branchId, name );
        try
        {
            xaContainer = xaFactory.newXaContainer( this, logFile,
                    new DummyCommandReaderFactory(),
                    new DummyCommandWriterFactory(),
                    ALLOW_ALL, new DummyTransactionFactory(), stateFactory, new TransactionInterceptorProviders(
                            Iterables.<TransactionInterceptorProvider>empty(),
                            new DependencyResolver.Adapter()
                            {
                                @Override
                                public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
                                {
                                    return type.cast( new Config( MapUtil.stringMap(
                                            GraphDatabaseSettings.intercept_committing_transactions.name(),
                                            Settings.FALSE,
                                            GraphDatabaseSettings.intercept_deserialized_transactions.name(),
                                            Settings.FALSE
                                            ) ) );
                                }
                            } ), false, Functions.<List<LogEntry>>identity() );
            xaContainer.openLogicalLog();
        }
        catch ( IOException e )
        {
            throw new InstantiationException( "" + e );
        }
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop() throws IOException
    {
        xaContainer.close();
        // cleanup dummy resource log
        //            deleteAllResourceFiles();
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

    static class DummyXaResource extends XaResourceHelpImpl
    {
        DummyXaResource( XaResourceManager xaRm )
        {
            super( xaRm, null );
        }

        @Override
        public boolean isSameRM( XAResource resource )
        {
            return resource instanceof DummyXaResource;
        }
    }

    static class DummyXaConnection extends XaConnectionHelpImpl
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

        public void enlistWithTx( TransactionManager tm ) throws Exception
        {
            tm.getTransaction().enlistResource( xaResource );
        }

        public void delistFromTx( TransactionManager tm ) throws Exception
        {
            tm.getTransaction().delistResource( xaResource,
                    XAResource.TMSUCCESS );
        }

        public int getTransactionId() throws Exception
        {
            return getTransaction().getIdentifier();
        }
    }

    private static class DummyCommand extends XaCommand
    {
        private int type = -1;

        DummyCommand( int type )
        {
            this.type = type;
        }

        public int getType()
        {
            return type;
        }

    }


    private static class DummyCommandReaderFactory implements XaCommandReaderFactory
    {
        @Override
        public XaCommandReader newInstance( byte logEntryVersion, final ByteBuffer buffer )
        {
            return new XaCommandReader()
            {
                @Override
                public XaCommand read( ReadableByteChannel channel ) throws IOException
                {
                    buffer.clear();
                    buffer.limit( 4 );
                    if ( channel.read( buffer ) == 4 )
                    {
                        buffer.flip();
                        return new DummyCommand( buffer.getInt() );
                    }
                    return null;
                }
            };
        }
    }

    private static class DummyCommandWriterFactory implements XaCommandWriterFactory
    {
        @Override
        public XaCommandWriter newInstance()
        {
            return new XaCommandWriter()
            {
                @Override
                public void write( XaCommand command, LogBuffer buffer ) throws IOException
                {
                    buffer.putInt( ((DummyCommand)command).getType() );
                }
            };
        }
    }

    private static class DummyTransaction extends XaTransaction
    {
        private final java.util.List<XaCommand> commandList = new java.util.ArrayList<XaCommand>();

        public DummyTransaction( XaLogicalLog log, TransactionState state )
        {
            super( log, state );
            setCommitTxId( 0 );
        }

        @Override
        public void doAddCommand( XaCommand command )
        {
            commandList.add( command );
        }

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

    static class DummyTransactionFactory extends XaTransactionFactory
    {
        @Override
        public XaTransaction create( long lastCommittedTxWhenTransactionStarted, TransactionState state )
        {
            return new DummyTransaction( getLogicalLog(), state );
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
}
