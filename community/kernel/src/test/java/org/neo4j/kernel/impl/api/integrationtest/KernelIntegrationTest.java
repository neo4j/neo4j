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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
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
    @SuppressWarnings( "deprecation" )
    protected GraphDatabaseAPI db;
    ThreadToStatementContextBridge statementContextSupplier;
    protected InwardKernel kernel;
    protected IndexingService indexingService;

    private KernelTransaction transaction;
    private DbmsOperations dbmsOperations;

    protected TokenWrite tokenWriteInNewTransaction() throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.writeToken() );
        return transaction.tokenWrite();
    }

    protected Write dataWriteInNewTransaction() throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.write() );
        return transaction.dataWrite();
    }

    protected SchemaWrite schemaWriteInNewTransaction() throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
        return transaction.schemaWrite();
    }

    protected Procedures procs() throws TransactionFailureException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() );
        return transaction.procedures();
    }

    protected KernelTransaction newTransaction() throws TransactionFailureException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() );
        return transaction;
    }

    protected KernelTransaction newTransaction( LoginContext loginContext ) throws TransactionFailureException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, loginContext );
        return transaction;
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
        kernel = db.getDependencyResolver().resolveDependency( InwardKernel.class );
        indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        statementContextSupplier = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        dbmsOperations = db.getDependencyResolver().resolveDependency( DbmsOperations.class );
    }

    protected GraphDatabaseService createGraphDatabase()
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
        if ( transaction != null && transaction.isOpen() )
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

    boolean nodeHasLabel( KernelTransaction transaction, long node, int label )
    {
        try ( NodeCursor cursor = transaction.cursors().allocateNodeCursor() )
        {
            transaction.dataRead().singleNode( node, cursor );
            return cursor.next() && cursor.labels().contains( label );
        }
    }

    boolean nodeHasProperty( KernelTransaction transaction, long node, int property )
    {
        try ( NodeCursor cursor = transaction.cursors().allocateNodeCursor();
              PropertyCursor properties = transaction.cursors().allocatePropertyCursor() )
        {
            transaction.dataRead().singleNode( node, cursor );
            if ( !cursor.next() )
            {
                return false;
            }
            else
            {
                cursor.properties( properties );
                while ( properties.next() )
                {
                    if ( properties.propertyKey() == property )
                    {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    Value nodeGetProperty( KernelTransaction transaction, long node, int property )
    {
        try ( NodeCursor cursor = transaction.cursors().allocateNodeCursor();
              PropertyCursor properties = transaction.cursors().allocatePropertyCursor() )
        {
            transaction.dataRead().singleNode( node, cursor );
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

    PrimitiveIntIterator nodeGetPropertyKeys( KernelTransaction transaction, long node )
    {
        try ( NodeCursor cursor = transaction.cursors().allocateNodeCursor();
              PropertyCursor properties = transaction.cursors().allocatePropertyCursor() )
        {
            PrimitiveIntSet props = Primitive.intSet();
            transaction.dataRead().singleNode( node, cursor );
            if ( cursor.next() )
            {
                cursor.properties( properties );
                while ( properties.next() )
                {
                    props.add( properties.propertyKey() );
                }
            }
            return props.iterator();
        }
    }

    Value relationshipGetProperty( KernelTransaction transaction, long relationship, int property )
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

    Iterator<Long> nodeGetRelationships( KernelTransaction transaction, long node, Direction direction )
    {
        return nodeGetRelationships( transaction, node, direction, null );
    }

    Iterator<Long> nodeGetRelationships( KernelTransaction transaction, long node, Direction direction, int[] types )
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

    protected int countNodes( KernelTransaction transaction )
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

    int countRelationships( KernelTransaction transaction )
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
}
