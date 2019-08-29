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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.Iterator;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.values.storable.Values.NO_VALUE;

@TestDirectoryExtension
public abstract class KernelIntegrationTest
{
    @Inject
    protected TestDirectory testDir;

    protected GraphDatabaseAPI db;
    ThreadToStatementContextBridge statementContextSupplier;
    protected Kernel kernel;
    protected IndexingService indexingService;

    private Transaction transaction;
    private DbmsOperations dbmsOperations;
    protected DependencyResolver dependencyResolver;
    protected DefaultValueMapper valueMapper;
    private DatabaseManagementService managementService;

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
        transaction = kernel.beginTransaction( implicit, AnonymousContext.full() );
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
    Resource captureTransaction()
    {
        Transaction tx = transaction;
        return () -> transaction = tx;
    }

    protected DbmsOperations dbmsOperations()
    {
        return dbmsOperations;
    }

    protected void commit() throws TransactionFailureException
    {
        transaction.commit();
        transaction = null;
    }

    protected void rollback() throws TransactionFailureException
    {
        transaction.rollback();
        transaction = null;
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
        managementService = createDatabaseService();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        dependencyResolver = db.getDependencyResolver();
        kernel = dependencyResolver.resolveDependency( Kernel.class );
        indexingService = dependencyResolver.resolveDependency( IndexingService.class );
        statementContextSupplier = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        dbmsOperations = dependencyResolver.resolveDependency( DbmsOperations.class );
        valueMapper = new DefaultValueMapper( dependencyResolver.resolveDependency( EmbeddedProxySPI.class ) );
    }

    protected DatabaseManagementService createDatabaseService()
    {
        TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder = configure( createGraphDatabaseFactory( testDir.storeDir() ) )
                .setFileSystem( testDir.getFileSystem() );
        return configure( databaseManagementServiceBuilder ).build();
    }

    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( File databaseRootDir )
    {
        return new TestDatabaseManagementServiceBuilder( databaseRootDir );
    }

    protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder )
    {
        return databaseManagementServiceBuilder;
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
        managementService.shutdown();
    }

    protected void restartDb() throws TransactionFailureException
    {
        stopDb();
        startDb();
    }

    static Value relationshipGetProperty( Transaction transaction, long relationship, int property )
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

    static Iterator<Long> nodeGetRelationships( Transaction transaction, long node, Direction direction )
    {
        return nodeGetRelationships( transaction, node, direction, null );
    }

    static Iterator<Long> nodeGetRelationships( Transaction transaction, long node, Direction direction, int[] types )
    {
        try ( NodeCursor cursor = transaction.cursors().allocateNodeCursor() )
        {
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
    }

    protected static int countNodes( Transaction transaction )
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

    public static int countRelationships( Transaction transaction )
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
