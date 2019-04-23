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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory30;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.tracking.TrackingIndexExtensionFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.index.schema.tracking.TrackingReadersIndexAccessor.numberOfClosedReaders;
import static org.neo4j.kernel.impl.index.schema.tracking.TrackingReadersIndexAccessor.numberOfOpenReaders;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class UniqueIndexSeekIT
{
    @Inject
    private TestDirectory directory;
    private DatabaseManagementService managementService;

    @ParameterizedTest
    @MethodSource( "indexProviderFactories" )
    void uniqueIndexSeekDoNotLeakIndexReaders( AbstractIndexProviderFactory providerFactory ) throws KernelException
    {
        TrackingIndexExtensionFactory indexExtensionFactory = new TrackingIndexExtensionFactory( providerFactory );
        GraphDatabaseAPI database = createDatabase( indexExtensionFactory, providerFactory.descriptor() );
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        Config config = dependencyResolver.resolveDependency( Config.class );
        try
        {

            Label label = label( "spaceship" );
            String nameProperty = "name";
            createUniqueConstraint( database, label, nameProperty );

            generateRandomData( database, label, nameProperty );

            assertNotNull( indexExtensionFactory.getIndexProvider( config.get( default_database ) ) );
            assertThat( numberOfClosedReaders(), greaterThan( 0L ) );
            assertThat( numberOfOpenReaders(), greaterThan( 0L ) );
            assertThat( numberOfClosedReaders(), closeTo( numberOfOpenReaders(), 1 ) );

            lockNodeUsingUniqueIndexSeek( database, label, nameProperty );

            assertThat( numberOfClosedReaders(), closeTo( numberOfOpenReaders(), 1 ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static Stream<AbstractIndexProviderFactory> indexProviderFactories()
    {
        return Stream.of(
                new NativeLuceneFusionIndexProviderFactory30(),
                new GenericNativeIndexProviderFactory() );
    }

    private static CombinableMatcher<Long> closeTo( long from, long delta )
    {
        return both( greaterThanOrEqualTo( from ) ).and( lessThanOrEqualTo( from + delta ) );
    }

    private GraphDatabaseAPI createDatabase( TrackingIndexExtensionFactory indexExtensionFactory, IndexProviderDescriptor descriptor )
    {
        managementService = new TestDatabaseManagementServiceBuilder( directory.storeDir() )
                .setExtensions( singletonList( indexExtensionFactory ) )
                .setConfig( default_schema_provider, descriptor.name() )
                .build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void lockNodeUsingUniqueIndexSeek( GraphDatabaseAPI database, Label label, String nameProperty ) throws KernelException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            ThreadToStatementContextBridge contextBridge = database.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction kernelTransaction = contextBridge.getKernelTransactionBoundToThisThread( true );
            TokenRead tokenRead = kernelTransaction.tokenRead();
            Read dataRead = kernelTransaction.dataRead();

            int labelId = tokenRead.nodeLabel( label.name() );
            int propertyId = tokenRead.propertyKey( nameProperty );
            IndexReference indexReference = kernelTransaction.schemaRead().index( labelId, propertyId );
            try ( NodeValueIndexCursor cursor = kernelTransaction.cursors().allocateNodeValueIndexCursor()  )
            {
                dataRead.lockingNodeUniqueIndexSeek( indexReference, cursor, IndexQuery.ExactPredicate.exact( propertyId, "value" ) );
            }
            transaction.success();
        }
    }

    private static void generateRandomData( GraphDatabaseAPI database, Label label, String nameProperty )
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

    private static void createUniqueConstraint( GraphDatabaseAPI database, Label label, String nameProperty )
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
