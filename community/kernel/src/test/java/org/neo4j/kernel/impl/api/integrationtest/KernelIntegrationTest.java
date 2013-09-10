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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.InvalidTransactionTypeException;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.TestGraphDatabaseBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public abstract class KernelIntegrationTest
{
    protected GraphDatabaseAPI db;
    protected KernelAPI kernel;
    protected ThreadToStatementContextBridge statementContextProvider;

    private Transaction beansTx;
    private Statement statement;
    private EphemeralFileSystemAbstraction fs;

    protected DataWriteOperations dataWriteOperationsInNewTransaction() throws InvalidTransactionTypeException
    {
        beansTx = db.beginTx();
        statement = statementContextProvider.statement();
        return statement.dataWriteOperations();
    }

    protected SchemaWriteOperations schemaWriteOperationsInNewTransaction() throws InvalidTransactionTypeException
    {
        beansTx = db.beginTx();
        statement = statementContextProvider.statement();
        return statement.schemaWriteOperations();
    }

    protected ReadOperations readOperationsInNewTransaction()
    {
        beansTx = db.beginTx();
        statement = statementContextProvider.statement();
        return statement.readOperations();
    }

    protected void commit()
    {
        statement.close();
        statement = null;
        beansTx.success();
        beansTx.finish();
    }

    protected void rollback()
    {
        statement.close();
        statement = null;
        beansTx.failure();
        beansTx.finish();
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
        fs.shutdown();
    }

    protected void startDb()
    {
        TestGraphDatabaseBuilder graphDatabaseFactory = (TestGraphDatabaseBuilder) new TestGraphDatabaseFactory().setFileSystem(fs).newImpermanentDatabaseBuilder();
        db = (GraphDatabaseAPI) graphDatabaseFactory.setConfig(GraphDatabaseSettings.cache_type,"none").newGraphDatabase();
        statementContextProvider = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );
        kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
    }

    protected void stopDb()
    {
        db.shutdown();
    }

    protected void restartDb()
    {
        stopDb();
        startDb();
    }

    protected NeoStore neoStore()
    {
        return ((NeoStoreXaDataSource)db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME )).getNeoStore();
    }
}
