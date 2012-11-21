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

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

import javax.naming.NamingException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.objectweb.jotm.Current;
import org.objectweb.jotm.Jotm;
import org.objectweb.jotm.TransactionResourceManager;

public class JOTMTransactionManager extends AbstractTransactionManager
{
    @Override
    public int getEventIdentifier()
    {
        return 0;
    }

    public static class Provider extends TransactionManagerProvider
    {
        public Provider()
        {
            super( NAME );
        }

        @Override
        public AbstractTransactionManager loadTransactionManager(
                String txLogDir, XaDataSourceManager xaDataSourceManager, KernelPanicEventGenerator kpe,
                TxHook rollbackHook, StringLogger msgLog,
                FileSystemAbstraction fileSystem, TransactionStateFactory stateFactory )
        {
            return new JOTMTransactionManager( xaDataSourceManager, stateFactory );
        }
    }

    public static final String NAME = "JOTM";

    private final TransactionManager current;
    private final Jotm jotm;
    private final XaDataSourceManager xaDataSourceManager;
    private final Map<Transaction, TransactionState> states = new HashMap<Transaction, TransactionState>();
    private final TransactionStateFactory stateFactory;

    private JOTMTransactionManager( XaDataSourceManager xaDataSourceManager, TransactionStateFactory stateFactory )
    {
        this.xaDataSourceManager = xaDataSourceManager;
        this.stateFactory = stateFactory;

        Registry registry = null;
        try
        {
            registry = LocateRegistry.getRegistry( 1099 );
        }
        catch ( RemoteException re )
        {
            // Nothing yet, we can still create it.
        }
        if ( registry == null )
        {
            try
            {
                registry = LocateRegistry.createRegistry( 1099 );
            }
            catch ( RemoteException re )
            {
                // Something is fishy here, plus it is impossible to continue.
                // So we die.
                throw new Error( re );
            }
        }
        try
        {
            jotm = new Jotm( true, false );
            current = jotm.getTransactionManager();
        }
        catch ( NamingException ne )
        {
            throw new Error( "Error during JOTM creation", ne );
        }
    }

    /**
     * Starts the registry and binds a JOTM instance to it. Registers the
     * resource adapters declared by the neo data source manager to get ready
     * for possible recovery.
     */
    @Override
    public void init()
    {
    }

    @Override
    public void begin() throws NotSupportedException, SystemException
    {
        current.begin();
        Transaction tx = getTransaction();
        states.put( tx, stateFactory.create() );
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException,
            HeuristicRollbackException, SecurityException,
            IllegalStateException, SystemException
    {
        current.commit();
    }

    @Override
    public int getStatus() throws SystemException
    {
        return current.getStatus();
    }

    @Override
    public Transaction getTransaction() throws SystemException
    {
        if ( current == null )
        {
            return null;
        }
        return current.getTransaction();
    }

    @Override
    public void resume( Transaction arg0 ) throws InvalidTransactionException,
            IllegalStateException, SystemException
    {
        current.resume( arg0 );
    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException,
            SystemException
    {
        current.rollback();
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        current.setRollbackOnly();
    }

    @Override
    public void setTransactionTimeout( int arg0 ) throws SystemException
    {
        current.setTransactionTimeout( arg0 );
    }

    @Override
    public Transaction suspend() throws SystemException
    {
        return current.suspend();
    }

	@Override
	public void start() throws Throwable
	{
	}

    /**
     * Stops the JOTM instance.
     */
    @Override
    public void stop()
    {
        jotm.stop();
    }

	@Override
	public void shutdown() throws Throwable
	{
	}
	
	public Jotm getJotmTxManager()
	{
	    return jotm;
	}

    @Override
    public void doRecovery() throws Throwable
    {
        TransactionResourceManager trm = new TransactionResourceManager()
        {
            @Override
            public void returnXAResource( String rmName, XAResource rmXares )
            {
                return;
            }
        };

        try
        {
            for ( XaDataSource xaDs : xaDataSourceManager.getAllRegisteredDataSources() )
            {
                Current.getTransactionRecovery().registerResourceManager( xaDs.getName(),
                        xaDs.getXaConnection().getXaResource(), xaDs.getName(), trm );
            }
            Current.getTransactionRecovery().startResourceManagerRecovery();
        }
        catch ( XAException e )
        {
            throw new Error( "Error registering xa datasource", e );
        }
    }

    @Override
    public TransactionState getTransactionState()
    {
        try
        {
            TransactionState state = states.get( getTransaction() );
            return state != null ? state : TransactionState.NO_STATE;
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }
}
