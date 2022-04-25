/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.store.format.StoreVersion.ALIGNED_V5_0;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.impl.store.format.BaseRecordFormats;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormat;
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

public abstract class PageAlignedTestFormat extends BaseRecordFormats implements RecordFormats.Factory {
    private final String name;
    private final String versionString;

    public PageAlignedTestFormat(String name, String versionString, int majorFormatVersion, int minorFormatVersion) {
        super(ALIGNED_V5_0, majorFormatVersion, minorFormatVersion, FormatFamily.aligned.formatCapability());
        this.name = name;
        this.versionString = versionString;
    }

    @Override
    public String storeVersion() {
        return versionString;
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
        return FormatFamily.aligned;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public RecordFormats[] compatibleVersionsForRollingUpgrade() {
        return new RecordFormats[0];
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean formatUnderDevelopment() {
        return true;
    }

    @ServiceProvider
    public static class WithMinorVersionBump extends PageAlignedTestFormat {

        public static final String NAME = "Page-Aligned-Format-With-Minor-Version-Bump";
        public static final String VERSION_STRING = "TAF1.2";

        public WithMinorVersionBump() {
            super(
                    NAME,
                    VERSION_STRING,
                    PageAlignedV5_0.RECORD_FORMATS.majorVersion(),
                    PageAlignedV5_0.RECORD_FORMATS.minorVersion() + 1);
        }

        @Override
        public RecordFormats newInstance() {
            return new WithMinorVersionBump();
        }
    }

    @ServiceProvider
    public static class WithMajorVersionBump extends PageAlignedTestFormat {

        public static final String NAME = "Page-Aligned-Format-With-Major-Version-Bump";
        public static final String VERSION_STRING = "TAF2.1";

        public WithMajorVersionBump() {
            super(
                    NAME,
                    VERSION_STRING,
                    PageAlignedV5_0.RECORD_FORMATS.majorVersion() + 1,
                    PageAlignedV5_0.RECORD_FORMATS.minorVersion());
        }

        @Override
        public RecordFormats newInstance() {
            return new WithMajorVersionBump();
        }
    }
}
