/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api;


import org.neo4j.internal.kernel.api.Modes;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.newapi.NewKernel;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_timeout;

/**
 * This is the Neo4j Kernel, an implementation of the Kernel API which is an internal component used by Cypher and the
 * Core API (the API under org.neo4j.graphdb).
 *
 * WARNING: This class is under transition.
 *
 * <h1>Structure</h1>
 *
 * The Kernel lets you start transactions. The transactions allow you to create "statements", which, in turn, operate
 * against the database. Statements and transactions are separate concepts due to isolation requirements. A single
 * cypher query will normally use one statement, and there can be multiple statements executed in one transaction.
 *
 * Please refer to the {@link KernelTransaction} javadoc for details.
 *
 */
public class KernelImpl extends LifecycleAdapter implements InwardKernel
{
    private final KernelTransactions transactions;
    private final TransactionHooks hooks;
    private final DatabaseHealth health;
    private final TransactionMonitor transactionMonitor;
    private final Procedures procedures;
    private final Config config;

    private final NewKernel newKernel;

    public KernelImpl( KernelTransactions transactionFactory, TransactionHooks hooks, DatabaseHealth health,
            TransactionMonitor transactionMonitor, Procedures procedures, Config config, StorageEngine engine )
    {
        this.transactions = transactionFactory;
        this.hooks = hooks;
        this.health = health;
        this.transactionMonitor = transactionMonitor;
        this.procedures = procedures;
        this.config = config;
        this.newKernel = new NewKernel( engine, this );
    }

    @Override
    public KernelTransaction newTransaction( Transaction.Type type, LoginContext loginContext )
            throws TransactionFailureException
    {
        return newTransaction( type, loginContext, config.get( transaction_timeout ).toMillis() );
    }

    @Override
    public KernelTransaction newTransaction( Transaction.Type type, LoginContext loginContext, long timeout ) throws
            TransactionFailureException
    {
        health.assertHealthy( TransactionFailureException.class );
        KernelTransaction transaction = transactions.newInstance( type, loginContext, timeout );
        transactionMonitor.transactionStarted();
        return transaction;
    }

    @Override
    public void registerTransactionHook( TransactionHook hook )
    {
        hooks.register( hook );
    }

    @Override
    public void registerProcedure( CallableProcedure procedure ) throws ProcedureException
    {
        procedures.register( procedure );
    }

    @Override
    public void registerUserFunction( CallableUserFunction function ) throws ProcedureException
    {
        procedures.register( function );
    }

    @Override
    public void registerUserAggregationFunction( CallableUserAggregationFunction function ) throws ProcedureException
    {
        procedures.register( function );
    }

    @Override
    public void start()
    {
        newKernel.start();
    }

    @Override
    public void stop()
    {
        newKernel.stop();
    }

    @Override
    public Session beginSession( LoginContext loginContext )
    {
        return newKernel.beginSession( loginContext );
    }

    @Override
    public Modes modes()
    {
        return newKernel;
    }
}
