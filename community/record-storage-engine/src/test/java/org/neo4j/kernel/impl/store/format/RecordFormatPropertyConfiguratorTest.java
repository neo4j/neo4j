/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.array_block_size;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.label_block_size;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.string_block_size;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_BLOCK_SIZE;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_LABEL_BLOCK_SIZE;
import static org.neo4j.configuration.GraphDatabaseSettings.MINIMAL_BLOCK_SIZE;
import static org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator.configureRecordFormat;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.NoRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.StoreFormatLimits;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

class RecordFormatPropertyConfiguratorTest {
    @Test
    void keepUserDefinedFormatConfig() {
        Config config = Config.defaults(string_block_size, 36);
        RecordFormats recordFormats = defaultFormat();
        configureRecordFormat(recordFormats, config);
        assertEquals(36, config.get(string_block_size).intValue(), "Should keep used specified value");
    }

    @Test
    void overrideDefaultValuesForCurrentFormat() {
        Config config = Config.defaults();
        int testHeaderSize = 17;
        ResizableRecordFormats recordFormats = new ResizableRecordFormats(testHeaderSize);

        configureRecordFormat(recordFormats, config);

        assertEquals(
                DEFAULT_BLOCK_SIZE - testHeaderSize,
                config.get(string_block_size).intValue());
        assertEquals(
                DEFAULT_BLOCK_SIZE - testHeaderSize,
                config.get(array_block_size).intValue());
        assertEquals(
                DEFAULT_LABEL_BLOCK_SIZE - testHeaderSize,
                config.get(label_block_size).intValue());
    }

    @Test
    void checkForMinimumBlockSize() {
        Config config = Config.defaults();
        int testHeaderSize = 60;
        ResizableRecordFormats recordFormats = new ResizableRecordFormats(testHeaderSize);

        var e = assertThrows(IllegalArgumentException.class, () -> configureRecordFormat(recordFormats, config));
        assertEquals(e.getMessage(), "Block size should be bigger then " + MINIMAL_BLOCK_SIZE);
    }

    private static class ResizableRecordFormats implements RecordFormats {
        private final int dynamicRecordHeaderSize;

        ResizableRecordFormats(int dynamicRecordHeaderSize) {
            this.dynamicRecordHeaderSize = dynamicRecordHeaderSize;
        }

        @Override
        public String introductionVersion() {
            return null;
        }

        @Override
        public int majorVersion() {
            return NO_GENERATION;
        }

        @Override
        public int minorVersion() {
            return NO_GENERATION;
        }

        @Override
        public RecordFormat<NodeRecord> node() {
            return null;
        }

        @Override
        public RecordFormat<RelationshipGroupRecord> relationshipGroup() {
            return null;
        }

        @Override
        public RecordFormat<RelationshipRecord> relationship() {
            return null;
        }

        @Override
        public RecordFormat<PropertyRecord> property() {
            return null;
        }

        @Override
        public RecordFormat<SchemaRecord> schema() {
            return null;
        }

        @Override
        public RecordFormat<LabelTokenRecord> labelToken() {
            return null;
        }

        @Override
        public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken() {
            return null;
        }

        @Override
        public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken() {
            return null;
        }

        @Override
        public RecordFormat<DynamicRecord> dynamic() {
            return new ResizableRecordFormat(dynamicRecordHeaderSize);
        }

        @Override
        public RecordFormat<MetaDataRecord> metaData() {
            return null;
        }

        @Override
        public Capability[] capabilities() {
            return new Capability[0];
        }

        @Override
        public boolean hasCapability(Capability capability) {
            return false;
        }

        @Override
        public FormatFamily getFormatFamily() {
            return FormatFamily.STANDARD;
        }

        @Override
        public boolean hasCompatibleCapabilities(RecordFormats other, CapabilityType type) {
            return false;
        }

        @Override
        public String name() {
            return getClass().getName();
        }

        @Override
        public boolean onlyForMigration() {
            return false;
        }

        @Override
        public StoreFormatLimits idLimits() {
            return null;
        }
    }

    private static class ResizableRecordFormat extends NoRecordFormat<DynamicRecord> {
        private final int headerSize;

        ResizableRecordFormat(int headerSize) {
            this.headerSize = headerSize;
        }

        @Override
        public int getRecordHeaderSize() {
            return headerSize;
        }
    }
}
