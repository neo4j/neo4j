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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.util.Iterator;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.values.storable.Values.NO_VALUE;

public abstract class KernelIntegrationTest
{
    protected final TestDirectory testDir = TestDirectory.testDirectory();
    protected final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir ).around( fileSystemRule );
    protected GraphDatabaseAPI db;
    ThreadToStatementContextBridge statementContextSupplier;
    protected Kernel kernel;
    protected IndexingService indexingService;

    private Transaction transaction;
    private DbmsOperations dbmsOperations;
    protected DependencyResolver dependencyResolver;

    protected TokenWrite tokenWriteInNewTransaction() throws KernelException
    {
        transaction = kernel.beginTransaction( implicit, AnonymousContext.writeToken() );
        return transaction.tokenWrite();
    }

    protected Write dataWriteInNewTransaction() throws KernelException
    {
        transaction = kernel.beginTransaction( implicit, AnonymousContext.write() );
        return transaction.dataWrite();
    }

    protected SchemaWrite schemaWriteInNewTransaction() throws KernelException
    {
        transaction = kernel.beginTransaction( implicit, AUTH_DISABLED );
        return transaction.schemaWrite();
    }

    protected Procedures procs() throws TransactionFailureException
    {
        transaction = kernel.beginTransaction( implicit, AnonymousContext.read() );
        return transaction.procedures();
    }

    protected Procedures procsSchema() throws TransactionFailureException
    {
        transaction = kernel.beginTransaction( KernelTransaction.Type.implicit, AnonymousContext.full() );
        return transaction.procedures();
    }

    protected Transaction newTransaction() throws TransactionFailureException
    {
        transaction = kernel.beginTransaction( implicit, AnonymousContext.read() );
        return transaction;
    }

    protected Transaction newTransaction( LoginContext loginContext ) throws TransactionFailureException
    {
        transaction = kernel.beginTransaction( implicit, loginContext );
        return transaction;
    }

    /**
     * Create a temporary section wherein other transactions can be started an committed, and after which the <em>current</em> transaction will be restored as
     * current.
     */
    protected Resource captureTransaction()
    {
        Transaction tx = transaction;
        return () ->
        {
            transaction = tx;
        };
    }

    protected DbmsOperations dbmsOperations()
    {
        return dbmsOperations;
    }

    protected void commit() throws TransactionFailureException
    {
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
        startDb();
    }

    @After
    public void cleanup() throws Exception
    {
        stopDb();
    }

    protected void startDb()
    {
        db = (GraphDatabaseAPI) createGraphDatabase();
        dependencyResolver = db.getDependencyResolver();
        kernel = dependencyResolver.resolveDependency( Kernel.class );
        indexingService = dependencyResolver.resolveDependency( IndexingService.class );
        statementContextSupplier = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        dbmsOperations = dependencyResolver.resolveDependency( DbmsOperations.class );
    }

    protected GraphDatabaseService createGraphDatabase()
    {
        GraphDatabaseBuilder graphDatabaseBuilder = configure( createGraphDatabaseFactory() )
                .setFileSystem( fileSystemRule.get() )
                .newEmbeddedDatabaseBuilder( testDir.storeDir() );
        return configure( graphDatabaseBuilder ).newGraphDatabase();
    }

    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return new TestGraphDatabaseFactory();
    }

    protected GraphDatabaseBuilder configure( GraphDatabaseBuilder graphDatabaseBuilder )
    {
        return graphDatabaseBuilder;
    }

    protected TestGraphDatabaseFactory configure( TestGraphDatabaseFactory factory )
    {
        return factory;
    }

    void dbWithNoCache() throws TransactionFailureException
    {
        stopDb();
        startDb();
    }

    private void stopDb() throws TransactionFailureException
    {
        if ( transaction != null && transaction.isOpen() )
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

    Value relationshipGetProperty( Transaction transaction, long relationship, int property )
    {
        try ( RelationshipScanCursor cursor = transaction.cursors().allocateRelationshipScanCursor();
              PropertyCursor properties = transaction.cursors().allocatePropertyCursor() )
        {
            transaction.dataRead().singleRelationship( relationship, cursor );
            if ( !cursor.next() )
            {
                return NO_VALUE;
            }
            else
            {
                cursor.properties( properties );
                while ( properties.next() )
                {
                    if ( properties.propertyKey() == property )
                    {
                        return properties.propertyValue();
                    }
                }
                return NO_VALUE;
            }
        }
    }

    Iterator<Long> nodeGetRelationships( Transaction transaction, long node, Direction direction )
    {
        return nodeGetRelationships( transaction, node, direction, null );
    }

    Iterator<Long> nodeGetRelationships( Transaction transaction, long node, Direction direction, int[] types )
    {
        NodeCursor cursor = transaction.cursors().allocateNodeCursor();
        transaction.dataRead().singleNode( node, cursor );
        if ( !cursor.next() )
        {
            return emptyIterator();
        }

        switch ( direction )
        {
        case OUTGOING:
            return outgoingIterator( transaction.cursors(), cursor, types,
                    ( id, startNodeId, typeId, endNodeId ) -> id );
        case INCOMING:
            return incomingIterator( transaction.cursors(), cursor, types,
                    ( id, startNodeId, typeId, endNodeId ) -> id );
        case BOTH:
            return allIterator( transaction.cursors(), cursor, types,
                    ( id, startNodeId, typeId, endNodeId ) -> id );
        default:
            throw new IllegalStateException( direction + " is not a valid direction" );
        }
    }

    protected int countNodes( Transaction transaction )
    {
        int result = 0;
        try ( NodeCursor cursor = transaction.cursors().allocateNodeCursor() )
        {
            transaction.dataRead().allNodesScan( cursor );
            while ( cursor.next() )
            {
                result++;
            }
        }
        return result;
    }

    static int countRelationships( Transaction transaction )
    {
        int result = 0;
        try ( RelationshipScanCursor cursor = transaction.cursors().allocateRelationshipScanCursor() )
        {
            transaction.dataRead().allRelationshipsScan( cursor );
            while ( cursor.next() )
            {
                result++;
            }
        }
        return result;
    }

    KernelImpl internalKernel()
    {
        return (KernelImpl)kernel;
    }
}
