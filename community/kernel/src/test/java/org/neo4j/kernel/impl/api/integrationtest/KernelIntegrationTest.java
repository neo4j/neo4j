/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ProcedureCallOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

@ExtendWith( TestDirectoryExtension.class )
public abstract class KernelIntegrationTest
{
    @Resource
    TestDirectory testDir;
    @Rule
    protected final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @SuppressWarnings( "deprecation" )
    protected GraphDatabaseAPI db;
    ThreadToStatementContextBridge statementContextSupplier;
    protected InwardKernel kernel;
    protected IndexingService indexingService;

    private KernelTransaction transaction;
    private Statement statement;
    private DbmsOperations dbmsOperations;

    protected Statement statementInNewTransaction( LoginContext loginContext ) throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, loginContext );
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

    protected KernelTransaction newTransaction() throws TransactionFailureException
    {
        return kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() );
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

    @BeforeEach
    public void setup()
    {
        startDb();
    }

    @AfterEach
    public void cleanup() throws Exception
    {
        stopDb();
    }

    private void startDb()
    {
        db = (GraphDatabaseAPI) createGraphDatabase();
        kernel = db.getDependencyResolver().resolveDependency( InwardKernel.class );
        indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        statementContextSupplier = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        dbmsOperations = db.getDependencyResolver().resolveDependency( DbmsOperations.class );
    }

    GraphDatabaseService createGraphDatabase()
    {
        GraphDatabaseBuilder graphDatabaseBuilder = new TestGraphDatabaseFactory().setFileSystem( fileSystemRule.get() )
                .newEmbeddedDatabaseBuilder( testDir.graphDbDir() );
        return configure( graphDatabaseBuilder ).newGraphDatabase();
    }

    protected GraphDatabaseBuilder configure( GraphDatabaseBuilder graphDatabaseBuilder )
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

    void restartDb() throws TransactionFailureException
    {
        stopDb();
        startDb();
    }
}
