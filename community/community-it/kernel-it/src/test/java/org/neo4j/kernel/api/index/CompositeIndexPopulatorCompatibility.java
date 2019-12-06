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
package org.neo4j.kernel.api.index;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class CompositeIndexPopulatorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    CompositeIndexPopulatorCompatibility( IndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype )
    {
        super( testSuite, prototype );
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends CompositeIndexPopulatorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexPrototype.forSchema( forLabel( 1000, 100, 200 ) ) );
        }

        @Test
        public void shouldProvidePopulatorThatAcceptsDuplicateEntries() throws Exception
        {
            // when
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
            withPopulator( indexProvider.getPopulator( descriptor, indexSamplingConfig, heapBufferFactory( 1024 ) ), p -> p.add( Arrays.asList(
                    add( 1, descriptor.schema(), Values.of( "v1" ), Values.of( "v2" ) ),
                    add( 2, descriptor.schema(), Values.of( "v1" ), Values.of( "v2" ) ) ) ) );

            // then
            try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( descriptor, indexSamplingConfig ) )
            {
                try ( IndexReader reader = accessor.newReader();
                      NodeValueIterator nodes = new NodeValueIterator() )
                {
                    reader.query( NULL_CONTEXT, nodes, IndexOrder.NONE, false, IndexQuery.exact( 1, "v1" ), IndexQuery.exact( 1, "v2" ) );
                    assertEquals( asSet( 1L, 2L ), PrimitiveLongCollections.toSet( nodes ) );
                }
            }
        }
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Unique extends CompositeIndexPopulatorCompatibility
    {
        Value value1 = Values.of( "value1" );
        Value value2 = Values.of( "value2" );
        Value value3 = Values.of( "value3" );
        int nodeId1 = 3;
        int nodeId2 = 4;

        public Unique( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, IndexPrototype.uniqueForSchema( forLabel( 1000, 100, 200 ) ) );
        }

        @Test
        public void shouldEnforceUniqueConstraintsDirectly() throws Exception
        {
            // when
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
            withPopulator( indexProvider.getPopulator( descriptor, indexSamplingConfig, heapBufferFactory( 1024 ) ), p ->
            {
                try
                {
                    p.add( Arrays.asList(
                            add( nodeId1, descriptor.schema(), value1, value2 ),
                            add( nodeId2, descriptor.schema(), value1, value2 ) ) );
                    TestNodePropertyAccessor propertyAccessor =
                            new TestNodePropertyAccessor( nodeId1, descriptor.schema(), value1, value2 );
                    propertyAccessor.addNode( nodeId2, descriptor.schema(), value1, value2 );
                    p.scanCompleted( PhaseTracker.nullInstance, jobScheduler );
                    p.verifyDeferredConstraints( propertyAccessor );

                    fail( "expected exception" );
                }
                // then
                catch ( IndexEntryConflictException conflict )
                {
                    assertEquals( nodeId1, conflict.getExistingNodeId() );
                    assertEquals( ValueTuple.of( value1, value2 ), conflict.getPropertyValues() );
                    assertEquals( nodeId2, conflict.getAddedNodeId() );
                }
            }, false );
        }

        @Test
        public void shouldNotRestrictUpdatesDifferingOnSecondProperty() throws Exception
        {
            // given
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
            withPopulator( indexProvider.getPopulator( descriptor, indexSamplingConfig, heapBufferFactory( 1024 ) ), p ->
            {
                // when
                p.add( Arrays.asList(
                        add( nodeId1, descriptor.schema(), value1, value2 ),
                        add( nodeId2, descriptor.schema(), value1, value3 ) ) );

                TestNodePropertyAccessor propertyAccessor =
                        new TestNodePropertyAccessor( nodeId1, descriptor.schema(), value1, value2 );
                propertyAccessor.addNode( nodeId2, descriptor.schema(), value1, value3 );

                // then this should pass fine
                p.verifyDeferredConstraints( propertyAccessor );
            } );
        }
    }
}
