/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.lowlimit;

import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public class LowLimit implements RecordFormats
{
    public static final RecordFormats RECORD_FORMATS = new LowLimit();
    public static final String STORE_VERSION = "v0.A.7";

    private final RecordFormat<NodeRecord> node = new NodeRecordFormat();
    private final DynamicRecordFormat dynamic = new DynamicRecordFormat();
    private final RecordFormat<RelationshipRecord> relationship = new RelationshipRecordFormat();
    private final RecordFormat<PropertyRecord> property = new PropertyRecordFormat();
    private final RecordFormat<LabelTokenRecord> labelToken = new LabelTokenRecordFormat();
    private final RecordFormat<PropertyKeyTokenRecord> propertyKeyToken = new PropertyKeyTokenRecordFormat();
    private final RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken =
            new RelationshipTypeTokenRecordFormat();
    private final RecordFormat<RelationshipGroupRecord> relationshipGroup = new RelationshipGroupRecordFormat();

    @Override
    public String storeVersion()
    {
        return STORE_VERSION;
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return node;
    }

    @Override
    public RecordFormat<DynamicRecord> dynamic()
    {
        return dynamic;
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return relationship;
    }

    @Override
    public RecordFormat<PropertyRecord> property()
    {
        return property;
    }

    @Override
    public RecordFormat<LabelTokenRecord> labelToken()
    {
        return labelToken;
    }

    @Override
    public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return propertyKeyToken;
    }

    @Override
    public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return relationshipTypeToken;
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return relationshipGroup;
    }

    @Override
    public String toString()
    {
        return "RecordFormat[" + storeVersion() + "]";
    }
}
