/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@RunWith( Parameterized.class )
public class BatchInsertIndexProviderTest
{
    private final GraphDatabaseSettings.SchemaIndex schemaIndex;
    private DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private TestDirectory storeDir = TestDirectory.testDirectory();
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
        Map<String,String> config = stringMap( default_schema_provider.name(), schemaIndex.providerIdentifier() );
        BatchInserter inserter = newBatchInserter( config );
        inserter.createDeferredSchemaIndex( TestLabels.LABEL_ONE ).on( "key" ).create();
        inserter.shutdown();
        GraphDatabaseService db = graphDatabaseService( storeDir.databaseDir(), config );
        try ( Transaction tx = db.beginTx() )
        {
            DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
            ThreadToStatementContextBridge threadToStatementContextBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction kernelTransaction = threadToStatementContextBridge.getKernelTransactionBoundToThisThread( true );
            TokenRead tokenRead = kernelTransaction.tokenRead();
            SchemaRead schemaRead = kernelTransaction.schemaRead();
            int labelId = tokenRead.nodeLabel( TestLabels.LABEL_ONE.name() );
            int propertyId = tokenRead.propertyKey( "key" );
            IndexReference index = schemaRead.index( labelId, propertyId );
            assertTrue( unexpectedIndexProviderMessage( index ), schemaIndex.providerIdentifier().contains( index.providerKey() ) );
            assertTrue( unexpectedIndexProviderMessage( index ), schemaIndex.providerIdentifier().contains( index.providerVersion() ) );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private BatchInserter newBatchInserter( Map<String,String> config ) throws Exception
    {
        return BatchInserters.inserter( storeDir.databaseDir(), fileSystemRule.get(), config );
    }

    private GraphDatabaseService graphDatabaseService( File storeDir, Map<String, String> config )
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fileSystemRule.get() );
        GraphDatabaseService db = factory.newImpermanentDatabaseBuilder( storeDir )
                // Shouldn't be necessary to set dense node threshold since it's a stick config
                .setConfig( config )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }

        return db;
    }

    private static String unexpectedIndexProviderMessage( IndexReference index )
    {
        return "Unexpected provider: key=" + index.providerKey() + ", version=" + index.providerVersion();
    }
}
