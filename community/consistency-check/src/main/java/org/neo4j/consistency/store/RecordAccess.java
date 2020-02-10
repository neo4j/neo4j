/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.store;

import java.util.Iterator;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

public interface RecordAccess
{
    RecordReference<SchemaRecord> schema( long id, PageCursorTracer cursorTracer );

    RecordReference<NodeRecord> node( long id, PageCursorTracer cursorTracer );

    RecordReference<RelationshipRecord> relationship( long id, PageCursorTracer cursorTracer );

    RecordReference<PropertyRecord> property( long id, PageCursorTracer cursorTracer );

    RecordReference<RelationshipTypeTokenRecord> relationshipType( int id, PageCursorTracer cursorTracer );

    RecordReference<PropertyKeyTokenRecord> propertyKey( int id, PageCursorTracer cursorTracer );

    RecordReference<DynamicRecord> string( long id, PageCursorTracer cursorTracer );

    RecordReference<DynamicRecord> array( long id, PageCursorTracer cursorTracer );

    RecordReference<DynamicRecord> relationshipTypeName( int id, PageCursorTracer cursorTracer );

    RecordReference<DynamicRecord> nodeLabels( long id, PageCursorTracer cursorTracer );

    RecordReference<LabelTokenRecord> label( int id, PageCursorTracer cursorTracer );

    RecordReference<DynamicRecord> labelName( int id, PageCursorTracer cursorTracer );

    RecordReference<DynamicRecord> propertyKeyName( int id, PageCursorTracer cursorTracer );

    RecordReference<RelationshipGroupRecord> relationshipGroup( long id, PageCursorTracer cursorTracer );

    // The following methods doesn't belong here, but makes code in the rest of the CC immensely easier

    Iterator<PropertyRecord> rawPropertyChain( long firstId, PageCursorTracer cursorTracer );

    boolean shouldCheck( long id, MultiPassStore store );

    CacheAccess cacheAccess();
}
