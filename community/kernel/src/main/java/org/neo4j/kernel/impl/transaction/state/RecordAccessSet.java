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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.schema.SchemaRule;

public interface RecordAccessSet
{
    RecordAccess<NodeRecord, Void> getNodeRecords();

    RecordAccess<PropertyRecord, PrimitiveRecord> getPropertyRecords();

    RecordAccess<RelationshipRecord, Void> getRelRecords();

    RecordAccess<RelationshipGroupRecord, Integer> getRelGroupRecords();

    RecordAccess<SchemaRecord, SchemaRule> getSchemaRuleChanges();

    RecordAccess<PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges();

    RecordAccess<LabelTokenRecord, Void> getLabelTokenChanges();

    RecordAccess<RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges();

    boolean hasChanges();

    int changeSize();

    void close();
}
