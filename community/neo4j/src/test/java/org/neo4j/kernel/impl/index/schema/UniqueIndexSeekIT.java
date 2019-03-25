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
package org.neo4j.kernel.impl.index.schema;

import org.hamcrest.core.CombinableMatcher;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.tracking.TrackingIndexExtensionFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.index.schema.tracking.TrackingReadersIndexAccessor.numberOfClosedReaders;
import static org.neo4j.kernel.impl.index.schema.tracking.TrackingReadersIndexAccessor.numberOfOpenReaders;

public class UniqueIndexSeekIT
{
    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Test
    public void uniqueIndexSeekDoNotLeakIndexReaders() throws KernelException
    {
        TrackingIndexExtensionFactory indexExtensionFactory = new TrackingIndexExtensionFactory();
        GraphDatabaseAPI database = createDatabase( indexExtensionFactory );
        try
        {

            Label label = label( "spaceship" );
            String nameProperty = "name";
            createUniqueConstraint( database, label, nameProperty );

            generateRandomData( database, label, nameProperty );

            assertNotNull( indexExtensionFactory.getIndexProvider() );
            assertThat( numberOfClosedReaders(), greaterThan( 0L ) );
            assertThat( numberOfOpenReaders(), greaterThan( 0L ) );
            assertThat( numberOfClosedReaders(), closeTo( numberOfOpenReaders(), 1 ) );

            lockNodeUsingUniqueIndexSeek( database, label, nameProperty );

            assertThat( numberOfClosedReaders(), closeTo( numberOfOpenReaders(), 1 ) );
        }
        finally
        {
            database.shutdown();
        }
    }

    private static CombinableMatcher<Long> closeTo( long from, long delta )
    {
        return both( greaterThanOrEqualTo( from ) ).and( lessThanOrEqualTo( from + delta ) );
    }

    private GraphDatabaseAPI createDatabase( TrackingIndexExtensionFactory indexExtensionFactory )
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                        .setKernelExtensions( singletonList( indexExtensionFactory ) ).newEmbeddedDatabaseBuilder( directory.graphDbDir() ).newGraphDatabase();
    }

    private void lockNodeUsingUniqueIndexSeek( GraphDatabaseAPI database, Label label, String nameProperty ) throws KernelException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            ThreadToStatementContextBridge contextBridge = database.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction kernelTransaction = contextBridge.getKernelTransactionBoundToThisThread( true );
            TokenRead tokenRead = kernelTransaction.tokenRead();
            Read dataRead = kernelTransaction.dataRead();

            int labelId = tokenRead.nodeLabel( label.name() );
            int propertyId = tokenRead.propertyKey( nameProperty );
            CapableIndexReference indexReference = kernelTransaction.schemaRead().index( labelId, propertyId );
            dataRead.lockingNodeUniqueIndexSeek( indexReference, IndexQuery.ExactPredicate.exact( propertyId, "value" ) );
            transaction.success();
        }
    }

    private void generateRandomData( GraphDatabaseAPI database, Label label, String nameProperty )
    {
        for ( int i = 0; i < 1000; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode( label );
                node.setProperty( nameProperty, "PlanetExpress" + i );
                transaction.success();
            }
        }
    }

    private void createUniqueConstraint( GraphDatabaseAPI database, Label label, String nameProperty )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().constraintFor( label ).assertPropertyIsUnique( nameProperty ).create();
            transaction.success();
        }
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            transaction.success();
        }
    }
}
