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
package org.neo4j.consistency.checking.full;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.test.rule.NeoStoresRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PropertyReaderTest
{
    @Rule
    public final NeoStoresRule storesRule = new NeoStoresRule( PropertyReaderTest.class,
            StoreType.NODE, StoreType.COUNTS,
            StoreType.PROPERTY, StoreType.PROPERTY_ARRAY, StoreType.PROPERTY_STRING );

    @Test
    public void shouldDetectAndAbortPropertyChainLoadingOnCircularReference() throws IOException
    {
        // given
        NeoStores neoStores = storesRule.builder().build();

        // Create property chain 1 --> 2 --> 3 --> 4
        //                             ↑           │
        //                             └───────────┘
        PropertyStore propertyStore = neoStores.getPropertyStore();
        PropertyRecord record = propertyStore.newRecord();
        // 1
        record.setId( 1 );
        record.initialize( true, -1, 2 );
        propertyStore.updateRecord( record );
        // 2
        record.setId( 2 );
        record.initialize( true, 1, 3 );
        propertyStore.updateRecord( record );
        // 3
        record.setId( 3 );
        record.initialize( true, 2, 4 );
        propertyStore.updateRecord( record );
        // 4
        record.setId( 4 );
        record.initialize( true, 3, 2 ); // <-- completing the circle
        propertyStore.updateRecord( record );

        // when
        PropertyReader reader = new PropertyReader( new StoreAccess( neoStores ) );
        try
        {
            reader.getPropertyRecordChain( 1 );
            fail( "Should have detected circular reference" );
        }
        catch ( PropertyReader.CircularPropertyRecordChainException e )
        {
            // then good
            assertEquals( 4, e.propertyRecordClosingTheCircle().getId() );
        }
    }
}
