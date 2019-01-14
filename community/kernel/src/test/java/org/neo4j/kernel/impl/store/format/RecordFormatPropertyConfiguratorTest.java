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
package org.neo4j.kernel.impl.store.format;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.NoRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatFamily;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_BLOCK_SIZE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_LABEL_BLOCK_SIZE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.MINIMAL_BLOCK_SIZE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.array_block_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.label_block_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.string_block_size;

public class RecordFormatPropertyConfiguratorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void keepUserDefinedFormatConfig()
    {
        Config config = Config.defaults( string_block_size, "36" );
        RecordFormats recordFormats = Standard.LATEST_RECORD_FORMATS;
        new RecordFormatPropertyConfigurator( recordFormats, config ).configure();
        assertEquals( "Should keep used specified value", 36, config.get( string_block_size ).intValue() );
    }

    @Test
    public void overrideDefaultValuesForCurrentFormat()
    {
        Config config = Config.defaults();
        int testHeaderSize = 17;
        ResizableRecordFormats recordFormats = new ResizableRecordFormats( testHeaderSize );

        new RecordFormatPropertyConfigurator( recordFormats, config ).configure();

        assertEquals( DEFAULT_BLOCK_SIZE - testHeaderSize, config.get( string_block_size ).intValue() );
        assertEquals( DEFAULT_BLOCK_SIZE - testHeaderSize, config.get( array_block_size ).intValue() );
        assertEquals( DEFAULT_LABEL_BLOCK_SIZE - testHeaderSize, config.get( label_block_size ).intValue() );
    }

    @Test
    public void checkForMinimumBlockSize()
    {
        Config config = Config.defaults();
        int testHeaderSize = 60;
        ResizableRecordFormats recordFormats = new ResizableRecordFormats( testHeaderSize );

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Block size should be bigger then " + MINIMAL_BLOCK_SIZE );

        new RecordFormatPropertyConfigurator( recordFormats, config ).configure();
    }

    private class ResizableRecordFormats implements RecordFormats
    {

        private int dynamicRecordHeaderSize;

        ResizableRecordFormats( int dynamicRecordHeaderSize )
        {
            this.dynamicRecordHeaderSize = dynamicRecordHeaderSize;
        }

        @Override
        public String storeVersion()
        {
            return null;
        }

        @Override
        public String introductionVersion()
        {
            return null;
        }

        @Override
        public int generation()
        {
            return 0;
        }

        @Override
        public RecordFormat<NodeRecord> node()
        {
            return null;
        }

        @Override
        public RecordFormat<RelationshipGroupRecord> relationshipGroup()
        {
            return null;
        }

        @Override
        public RecordFormat<RelationshipRecord> relationship()
        {
            return null;
        }

        @Override
        public RecordFormat<PropertyRecord> property()
        {
            return null;
        }

        @Override
        public RecordFormat<LabelTokenRecord> labelToken()
        {
            return null;
        }

        @Override
        public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken()
        {
            return null;
        }

        @Override
        public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken()
        {
            return null;
        }

        @Override
        public RecordFormat<DynamicRecord> dynamic()
        {
            return new ResizableRecordFormat( dynamicRecordHeaderSize );
        }

        @Override
        public RecordFormat<MetaDataRecord> metaData()
        {
            return null;
        }

        @Override
        public Capability[] capabilities()
        {
            return new Capability[0];
        }

        @Override
        public boolean hasCapability( Capability capability )
        {
            return false;
        }

        @Override
        public FormatFamily getFormatFamily()
        {
            return StandardFormatFamily.INSTANCE;
        }

        @Override
        public boolean hasCompatibleCapabilities( RecordFormats other, CapabilityType type )
        {
            return false;
        }

        @Override
        public String name()
        {
            return getClass().getName();
        }
    }

    private class ResizableRecordFormat extends NoRecordFormat<DynamicRecord>
    {
        private int headerSize;

        ResizableRecordFormat( int headerSize )
        {
            this.headerSize = headerSize;
        }

        @Override
        public int getRecordHeaderSize()
        {
            return headerSize;
        }
    }
}
