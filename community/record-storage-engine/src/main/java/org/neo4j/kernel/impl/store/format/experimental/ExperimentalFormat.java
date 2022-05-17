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
package org.neo4j.kernel.impl.store.format.experimental;

import static org.neo4j.configuration.GraphDatabaseSettings.DatabaseRecordFormat;
import static org.neo4j.kernel.impl.store.format.StoreVersion.EXPERIMENTAL;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.impl.store.format.BaseRecordFormats;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.RecordStorageCapability;
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

/**
 * Experimental format which uses little-endian byte order.
 *
 * This format has its own experimental format family to avoid interference with other formats in testing.
 * Not publicly visible, i.e. there is no {@link DatabaseRecordFormat} option to select it.
 * It can be selected using {@link GraphDatabaseInternalSettings#select_specific_record_format} setting.
 */
public class ExperimentalFormat extends BaseRecordFormats {
    public static final RecordFormats RECORD_FORMATS = new ExperimentalFormat();
    public static final String NAME = "experimental";

    private ExperimentalFormat() {
        super(EXPERIMENTAL, 1, 0, FormatFamily.experimental.formatCapability(), RecordStorageCapability.LITTLE_ENDIAN);
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
        return FormatFamily.experimental;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public RecordFormats[] compatibleVersionsForRollingUpgrade() {
        return new RecordFormats[0];
    }

    @Override
    public boolean formatUnderDevelopment() {
        // family requires that there are at least one format that is not under developement
        return false;
    }
}
