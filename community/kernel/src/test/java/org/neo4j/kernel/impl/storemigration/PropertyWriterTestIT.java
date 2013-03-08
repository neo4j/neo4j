/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.CommonFactories.defaultIdGeneratorFactory;
import static org.neo4j.kernel.CommonFactories.defaultTxHook;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

public class PropertyWriterTestIT
{
    private NeoStore neoStore;

    private PropertyStore newPropertyStore() throws IOException
    {
        Config config = MigrationTestUtils.defaultConfig();
        File outputDir = new File( "target/outputDatabase" );
        File fileName = new File( outputDir, "neostore" );
        StoreFactory storeFactory = new StoreFactory( config, defaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs.get(),
                StringLogger.DEV_NULL, defaultTxHook() );
        neoStore = storeFactory.createNeoStore( fileName );
        return neoStore.getPropertyStore();
    }

    @After
    public void close()
    {
        neoStore.close();
    }

    @Test
    public void shouldPackASeriesOfPropertiesIntoAPropertyRecord() throws IOException
    {
        PropertyStore propertyStore = newPropertyStore();

        assertEquals( 0, propertyStore.getHighId() );

        PropertyWriter propertyWriter = new PropertyWriter( propertyStore );
        ArrayList<Pair<Integer, Object>> properties = new ArrayList<Pair<Integer, Object>>();
        properties.add( Pair.of( 0, (Object) 1234 ) );
        properties.add( Pair.of( 1, (Object) 5678 ) );
        long propertyRecordId = propertyWriter.writeProperties( properties );

        assertEquals( 1, propertyStore.getHighId() );
        assertEquals( 0, propertyRecordId );

        List<PropertyBlock> propertyBlocks = new ArrayList<PropertyBlock>( propertyStore.getRecord( propertyRecordId
        ).getPropertyBlocks() );
        assertEquals( 0, propertyBlocks.get( 0 ).getKeyIndexId() );
        assertEquals( 1234, propertyBlocks.get( 0 ).getSingleValueInt() );
        assertEquals( 1, propertyBlocks.get( 1 ).getKeyIndexId() );
        assertEquals( 5678, propertyBlocks.get( 1 ).getSingleValueInt() );

        propertyStore.close();
    }

    @Test
    public void shouldStoreMultiplePropertiesAcrossASeriesOfRecords() throws IOException
    {
        final int OneBlockPropertyBlockCount = 100;
        PropertyStore propertyStore = newPropertyStore();

        assertEquals( 0, propertyStore.getHighId() );

        PropertyWriter propertyWriter = new PropertyWriter( propertyStore );
        ArrayList<Pair<Integer, Object>> properties = new ArrayList<Pair<Integer, Object>>();
        for ( int i = 0; i < OneBlockPropertyBlockCount; i++ )
        {
            properties.add( Pair.of( i, (Object) i ) );
        }
        long propertyRecordId = propertyWriter.writeProperties( properties );

        assertEquals(
                OneBlockPropertyBlockCount / PropertyType.getPayloadSizeLongs(),
                propertyStore.getHighId() );
        assertEquals( 0, propertyRecordId );

        List<PropertyBlock> propertyBlocks = new ArrayList<PropertyBlock>();

        PropertyRecord propertyRecord = propertyStore.getRecord( 0 );
        propertyBlocks.addAll( propertyStore.getRecord( propertyRecordId ).getPropertyBlocks() );
        while ( propertyRecord.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            long currentRecordId = propertyRecord.getId();
            propertyRecord = propertyStore.getRecord( propertyRecord.getNextProp() );
            assertEquals( currentRecordId, propertyRecord.getPrevProp() );
            propertyBlocks.addAll( propertyStore.getRecord( propertyRecordId ).getPropertyBlocks() );
        }

        assertEquals( OneBlockPropertyBlockCount, propertyBlocks.size() );

        propertyStore.close();
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
