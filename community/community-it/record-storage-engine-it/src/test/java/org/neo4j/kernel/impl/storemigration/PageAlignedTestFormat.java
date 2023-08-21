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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.store.format.StoreVersion.ALIGNED_V5_0;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.impl.store.format.BaseRecordFormats;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormatFamilyCapability;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV5_0;
import org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.LabelTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyKeyTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipGroupRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipTypeTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.SchemaRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

public abstract class PageAlignedTestFormat extends BaseRecordFormats {
    private final String name;
    private final int majorFormatVersion;
    private final int minorFormatVersion;

    protected PageAlignedTestFormat(String name, int majorFormatVersion, int minorFormatVersion) {
        super(ALIGNED_V5_0, new RecordFormatFamilyCapability(FormatFamily.ALIGNED));
        this.name = name;
        this.majorFormatVersion = majorFormatVersion;
        this.minorFormatVersion = minorFormatVersion;
    }

    public int majorVersion() {
        return majorFormatVersion;
    }

    @Override
    public int minorVersion() {
        return minorFormatVersion;
    }

    @Override
    public RecordFormat<NodeRecord> node() {
        return new NodeRecordFormat(true);
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup() {
        return new RelationshipGroupRecordFormat(true);
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship() {
        return new RelationshipRecordFormat(true);
    }

    @Override
    public RecordFormat<PropertyRecord> property() {
        return new PropertyRecordFormat(true);
    }

    @Override
    public RecordFormat<LabelTokenRecord> labelToken() {
        return new LabelTokenRecordFormat(true);
    }

    @Override
    public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken() {
        return new PropertyKeyTokenRecordFormat(true);
    }

    @Override
    public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken() {
        return new RelationshipTypeTokenRecordFormat(true);
    }

    @Override
    public RecordFormat<DynamicRecord> dynamic() {
        return new DynamicRecordFormat(true);
    }

    @Override
    public RecordFormat<SchemaRecord> schema() {
        return new SchemaRecordFormat(true);
    }

    @Override
    public FormatFamily getFormatFamily() {
        return FormatFamily.ALIGNED;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean formatUnderDevelopment() {
        return true;
    }

    public static class WithMinorVersionBump extends PageAlignedTestFormat {
        public static final RecordFormats RECORD_FORMATS = new WithMinorVersionBump();
        public static final String NAME = "Page-Aligned-Format-With-Minor-Version-Bump";

        private WithMinorVersionBump() {
            super(
                    NAME,
                    PageAlignedV5_0.RECORD_FORMATS.majorVersion(),
                    PageAlignedV5_0.RECORD_FORMATS.minorVersion() + 1);
        }

        @ServiceProvider
        public static class Factory implements RecordFormats.Factory {
            @Override
            public String getName() {
                return NAME;
            }

            @Override
            public RecordFormats getInstance() {
                return RECORD_FORMATS;
            }
        }
    }

    public static class WithMajorVersionBump extends PageAlignedTestFormat {
        public static final RecordFormats RECORD_FORMATS = new WithMajorVersionBump();
        public static final String NAME = "Page-Aligned-Format-With-Major-Version-Bump";

        private WithMajorVersionBump() {
            super(
                    NAME,
                    PageAlignedV5_0.RECORD_FORMATS.majorVersion() + 1,
                    PageAlignedV5_0.RECORD_FORMATS.minorVersion());
        }

        @ServiceProvider
        public static class Factory implements RecordFormats.Factory {
            @Override
            public String getName() {
                return NAME;
            }

            @Override
            public RecordFormats getInstance() {
                return RECORD_FORMATS;
            }
        }
    }
}
