/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.test.Race;
import org.neo4j.test.rule.NeoStoresRule;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdGeneratorFactory;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PropertyEncoderStepTest
{
    private static final String LONG_STRING = StringUtils.repeat( "12%$heya", 40 );

    @Rule
    public final NeoStoresRule neoStoresRule = new NeoStoresRule( getClass(),
            StoreType.PROPERTY, StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
            StoreType.PROPERTY_STRING, StoreType.PROPERTY_ARRAY );

    @Test
    public void shouldAssignCorrectIdsOnParallelExecution() throws Throwable
    {
        StageControl control = mock( StageControl.class );
        int batchSize = 100;
        Configuration config = new Configuration()
        {
            @Override
            public int batchSize()
            {
                return batchSize;
            }
        };
        NeoStores stores = neoStoresRule.builder().with( fs -> new BatchingIdGeneratorFactory( fs ) ).build();
        BatchingPropertyKeyTokenRepository keyRepository =
                new BatchingPropertyKeyTokenRepository( stores.getPropertyKeyTokenStore() );
        PropertyStore propertyStore = stores.getPropertyStore();
        PropertyEncoderStep<NodeRecord,InputNode> encoder =
                new PropertyEncoderStep<>( control, config, keyRepository, propertyStore );
        BatchCollector<Batch<InputNode,NodeRecord>> sender = new BatchCollector<>();

        // WHEN
        Race race = new Race();
        for ( int i = 0; i < Runtime.getRuntime().availableProcessors(); i++ )
        {
            int id = i;
            race.addContestant( () -> encoder.process( batch( id, batchSize ), sender ) );
        }
        race.go();

        assertUniqueIds( sender.getBatches() );
    }

    private void assertUniqueIds( List<Batch<InputNode,NodeRecord>> batches )
    {
        PrimitiveLongSet ids = Primitive.longSet( 1_000 );
        PrimitiveLongSet stringIds = Primitive.longSet( 100 );
        PrimitiveLongSet arrayIds = Primitive.longSet( 100 );
        for ( Batch<InputNode,NodeRecord> batch : batches )
        {
            for ( PropertyRecord[] records : batch.propertyRecords )
            {
                for ( PropertyRecord record : records )
                {
                    assertTrue( ids.add( record.getId() ) );
                    for ( PropertyBlock block : record )
                    {
                        for ( DynamicRecord dynamicRecord : block.getValueRecords() )
                        {
                            switch ( dynamicRecord.getType() )
                            {
                            case STRING:
                                assertTrue( stringIds.add( dynamicRecord.getId() ) );
                                break;
                            case ARRAY:
                                assertTrue( arrayIds.add( dynamicRecord.getId() ) );
                                break;
                            default:
                                fail( "Unexpected property type " + dynamicRecord.getType() );
                            }
                        }
                    }
                }
            }
        }
    }

    protected Batch<InputNode,NodeRecord> batch( int id, int batchSize )
    {
        InputNode[] input = new InputNode[batchSize];
        NodeRecord[] records = new NodeRecord[batchSize];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < batchSize; i++ )
        {
            String value = id + "_" + i;
            if ( random.nextFloat() < 0.01 )
            {
                value += LONG_STRING;
            }
            input[i] = new InputNode( "source", 0, 0, null,
                    new Object[] {"key", value}, null, InputNode.NO_LABELS, null );
            records[i] = new NodeRecord( -1 );
        }
        Batch<InputNode,NodeRecord> batch = new Batch<>( input );
        batch.records = records;
        return batch;
    }
}
