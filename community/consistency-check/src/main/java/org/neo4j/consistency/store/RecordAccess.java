/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.store;

import java.util.Iterator;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public interface RecordAccess
{
    RecordReference<DynamicRecord> schema( final long id );

    RecordReference<NodeRecord> node( final long id );

    RecordReference<RelationshipRecord> relationship( final long id );

    RecordReference<PropertyRecord> property( final long id );

    RecordReference<RelationshipTypeTokenRecord> relationshipType( final int id );

    RecordReference<PropertyKeyTokenRecord> propertyKey( final int id );

    RecordReference<DynamicRecord> string( final long id );

    RecordReference<DynamicRecord> array( final long id );

    RecordReference<DynamicRecord> relationshipTypeName( final int id );

    RecordReference<DynamicRecord> nodeLabels( final long id );

    RecordReference<LabelTokenRecord> label( final int id );

    RecordReference<DynamicRecord> labelName( final int id );

    RecordReference<DynamicRecord> propertyKeyName( final int id );

    RecordReference<NeoStoreRecord> graph();

    RecordReference<RelationshipGroupRecord> relationshipGroup( long id );

    // The following methods doesn't belong here, but makes code in the rest of the CC immensely easier

    Iterator<PropertyRecord> rawPropertyChain( long firstId );

    boolean shouldCheck( long id, MultiPassStore store );

    CacheAccess cacheAccess();
}
