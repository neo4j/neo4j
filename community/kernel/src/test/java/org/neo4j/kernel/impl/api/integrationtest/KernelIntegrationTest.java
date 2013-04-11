/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.test.ImpermanentGraphDatabase;

public class KernelIntegrationTest
{

    public GraphDatabaseAPI db;
    public StatementContext statement;
    public KernelAPI kernel;
    public ThreadToStatementContextBridge statementContextProvider;

    private Transaction beansTx;
    private TransactionContext tx;

    public TransactionContext newTransaction()
    {
        beansTx = db.beginTx();
        tx = kernel.newTransactionContext();
        statement = statementContextProvider.getCtxForWriting();
        return tx;
    }

    public void commit()
    {
        statement.close();
        beansTx.success();
        beansTx.finish();
    }

    public void rollback()
    {
        beansTx.failure();
        beansTx.finish();
    }

    @Before
    public void before() throws Exception
    {
        db = new ImpermanentGraphDatabase();
        statementContextProvider = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );
        kernel = db.getDependencyResolver().resolveDependency(
                KernelAPI.class );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

}
