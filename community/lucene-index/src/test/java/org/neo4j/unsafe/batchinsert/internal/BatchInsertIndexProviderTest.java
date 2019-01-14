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
package org.neo4j.unsafe.batchinsert.internal;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexValueTestUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@RunWith( Parameterized.class )
public class BatchInsertIndexProviderTest
{
    private final GraphDatabaseSettings.SchemaIndex schemaIndex;
    private DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private TestDirectory storeDir = TestDirectory.testDirectory( getClass() );
    private PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( storeDir ).around( fileSystemRule ).around( pageCacheRule );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<GraphDatabaseSettings.SchemaIndex> data()
    {
        return Arrays.asList(
                GraphDatabaseSettings.SchemaIndex.LUCENE10,
                GraphDatabaseSettings.SchemaIndex.NATIVE10,
                GraphDatabaseSettings.SchemaIndex.NATIVE20
        );
    }

    public BatchInsertIndexProviderTest( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        this.schemaIndex = schemaIndex;
    }

    @Test
    public void batchInserterShouldUseConfiguredIndexProvider() throws Exception
    {
        Config config = Config.defaults( stringMap( default_schema_provider.name(), schemaIndex.providerName() ) );
        BatchInserter inserter = newBatchInserter( config );
        inserter.createDeferredSchemaIndex( TestLabels.LABEL_ONE ).on( "key" ).create();
        inserter.shutdown();
        GraphDatabaseService db = graphDatabaseService( inserter.getStoreDir(), config );
        awaitIndexesOnline( db );
        try ( Transaction tx = db.beginTx() )
        {
            DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
            ThreadToStatementContextBridge threadToStatementContextBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction kernelTransaction = threadToStatementContextBridge.getKernelTransactionBoundToThisThread( true );
            TokenRead tokenRead = kernelTransaction.tokenRead();
            SchemaRead schemaRead = kernelTransaction.schemaRead();
            int labelId = tokenRead.nodeLabel( TestLabels.LABEL_ONE.name() );
            int propertyId = tokenRead.propertyKey( "key" );
            CapableIndexReference index = schemaRead.index( labelId, propertyId );
            assertTrue( unexpectedIndexProviderMessage( index ), schemaIndex.providerName().contains( index.providerKey() ) );
            assertTrue( unexpectedIndexProviderMessage( index ), schemaIndex.providerName().contains( index.providerVersion() ) );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldPopulateIndexWithUniquePointsThatCollideOnSpaceFillingCurve() throws Exception
    {
        Config config = Config.defaults( stringMap( default_schema_provider.name(), schemaIndex.providerName() ) );
        BatchInserter inserter = newBatchInserter( config );
        Pair<PointValue,PointValue> collidingPoints = SpatialIndexValueTestUtil.pointsWithSameValueOnSpaceFillingCurve( config );
        inserter.createNode( MapUtil.map( "prop", collidingPoints.first() ), TestLabels.LABEL_ONE );
        inserter.createNode( MapUtil.map( "prop", collidingPoints.other() ), TestLabels.LABEL_ONE );
        inserter.createDeferredConstraint( TestLabels.LABEL_ONE ).assertPropertyIsUnique( "prop" ).create();
        inserter.shutdown();

        GraphDatabaseService db = graphDatabaseService( inserter.getStoreDir(), config );
        try
        {
            awaitIndexesOnline( db );
            try ( Transaction tx = db.beginTx() )
            {
                assertSingleCorrectHit( db, collidingPoints.first() );
                assertSingleCorrectHit( db, collidingPoints.other() );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldThrowWhenPopulatingWithNonUniquePoints() throws Exception
    {
        Config config = Config.defaults( stringMap( default_schema_provider.name(), schemaIndex.providerName() ) );
        BatchInserter inserter = newBatchInserter( config );
        PointValue point = Values.pointValue( CoordinateReferenceSystem.WGS84, 0.0, 0.0 );
        inserter.createNode( MapUtil.map( "prop", point ), TestLabels.LABEL_ONE );
        inserter.createNode( MapUtil.map( "prop", point ), TestLabels.LABEL_ONE );
        inserter.createDeferredConstraint( TestLabels.LABEL_ONE ).assertPropertyIsUnique( "prop" ).create();
        try
        {
            inserter.shutdown();
            fail( "Should have failed" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause(), Matchers.instanceOf( IndexEntryConflictException.class ) );
        }

        GraphDatabaseService db = graphDatabaseService( inserter.getStoreDir(), config );
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<IndexDefinition> indexes = db.schema().getIndexes().iterator();
            assertTrue( indexes.hasNext() );
            IndexDefinition index = indexes.next();
            Schema.IndexState indexState = db.schema().getIndexState( index );
            assertEquals( Schema.IndexState.FAILED, indexState );
            assertFalse( indexes.hasNext() );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private void assertSingleCorrectHit( GraphDatabaseService db, PointValue point )
    {
        ResourceIterator<Node> nodes = db.findNodes( TestLabels.LABEL_ONE, "prop", point );
        assertTrue( nodes.hasNext() );
        Node node = nodes.next();
        Object prop = node.getProperty( "prop" );
        assertEquals( point, prop );
        assertFalse( nodes.hasNext() );
    }

    private BatchInserter newBatchInserter( Config config ) throws Exception
    {
        return BatchInserters.inserter( storeDir.absolutePath(), fileSystemRule.get(), config.getRaw() );
    }

    private GraphDatabaseService graphDatabaseService( String storeDir, Config config )
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fileSystemRule.get() );
        return factory.newImpermanentDatabaseBuilder( new File( storeDir ) )
                // Shouldn't be necessary to set dense node threshold since it's a stick config
                .setConfig( config.getRaw() )
                .newGraphDatabase();
    }

    private void awaitIndexesOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private String unexpectedIndexProviderMessage( CapableIndexReference index )
    {
        return "Unexpected provider: key=" + index.providerKey() + ", version=" + index.providerVersion();
    }
}
