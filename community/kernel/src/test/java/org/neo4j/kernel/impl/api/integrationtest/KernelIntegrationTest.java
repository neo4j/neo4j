/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ProcedureCallOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;

public abstract class KernelIntegrationTest
{
    @SuppressWarnings("deprecation")
    protected GraphDatabaseAPI db;
    ThreadToStatementContextBridge statementContextSupplier;
    protected KernelAPI kernel;
    protected IndexingService indexingService;

    private KernelTransaction transaction;
    private Statement statement;
    private EphemeralFileSystemAbstraction fs;
    private DbmsOperations dbmsOperations;

    protected Statement statementInNewTransaction( SecurityContext securityContext ) throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, securityContext );
        statement = transaction.acquireStatement();
        return statement;
    }

    protected TokenWriteOperations tokenWriteOperationsInNewTransaction() throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.writeToken() );
        statement = transaction.acquireStatement();
        return statement.tokenWriteOperations();
    }

    protected DataWriteOperations dataWriteOperationsInNewTransaction() throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.write() );
        statement = transaction.acquireStatement();
        return statement.dataWriteOperations();
    }

    protected SchemaWriteOperations schemaWriteOperationsInNewTransaction() throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        statement = transaction.acquireStatement();
        return statement.schemaWriteOperations();
    }

    protected ProcedureCallOperations procedureCallOpsInNewTx() throws TransactionFailureException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() );
        statement = transaction.acquireStatement();
        return statement.procedureCallOperations();
    }

    protected ReadOperations readOperationsInNewTransaction() throws TransactionFailureException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() );
        statement = transaction.acquireStatement();
        return statement.readOperations();
    }

    protected DbmsOperations dbmsOperations()
    {
        return dbmsOperations;
    }

    protected void commit() throws TransactionFailureException
    {
        statement.close();
        statement = null;
        transaction.success();
        try
        {
            transaction.close();
        }
        finally
        {
            transaction = null;
        }
    }

    protected void rollback() throws TransactionFailureException
    {
        statement.close();
        statement = null;
        transaction.failure();
        try
        {
            transaction.close();
        }
        finally
        {
            transaction = null;
        }
    }

    @Before
    public void setup()
    {
        fs = new EphemeralFileSystemAbstraction();
        startDb();
    }

    @After
    public void cleanup() throws Exception
    {
        stopDb();
        fs.close();
    }

    protected void startDb()
    {
        db = (GraphDatabaseAPI) createGraphDatabase( fs );
        kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        statementContextSupplier = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        dbmsOperations = db.getDependencyResolver().resolveDependency( DbmsOperations.class );
    }

    protected GraphDatabaseService createGraphDatabase( EphemeralFileSystemAbstraction fs )
    {
        TestGraphDatabaseBuilder graphDatabaseBuilder = (TestGraphDatabaseBuilder) new TestGraphDatabaseFactory()
                .setFileSystem( fs )
                .newImpermanentDatabaseBuilder();
        return configure( graphDatabaseBuilder ).newGraphDatabase();
    }

    protected TestGraphDatabaseBuilder configure( TestGraphDatabaseBuilder graphDatabaseBuilder )
    {
        return graphDatabaseBuilder;
    }

    void dbWithNoCache() throws TransactionFailureException
    {
        stopDb();
        startDb();
    }

    private void stopDb() throws TransactionFailureException
    {
        if ( transaction != null )
        {
            transaction.close();
        }
        db.shutdown();
    }

    protected void restartDb() throws TransactionFailureException
    {
        stopDb();
        startDb();
    }
}
