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
package org.neo4j.legacy.consistency.store;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * A {@link RecordAccess} for use with incremental checking. Provides access to
 * <p>
 * {@link #previousNode(long) Node}, {@link #previousRelationship(long) Relationship}, and {@link
 * #previousProperty(long) Property} are the only record types one might need a previous version of when checking
 * another type of record.
 * <p>
 * Getting the new version of a record is an operation that can always be performed without any I/O, therefore these
 * return the records immediately, instead of returning {@link RecordReference} objects. New versions of records can be
 * retrieved for {@link #changedNode(long)} Node}, {@link #changedRelationship(long)} (long) Relationship}, {@link
 * #changedProperty(long)} (long) Property}, {@link #changedString(long) String property blocks}, and {@link
 * #changedArray(long) Array property blocks}, these are the only types of records for which there is a need to get the
 * new version while checking another type of record. The methods returning new versions of records return
 * <code>null</code> if the record has not been changed.
 */
public interface DiffRecordAccess extends RecordAccess
{
    RecordReference<NodeRecord> previousNode( long id );

    RecordReference<RelationshipRecord> previousRelationship( long id );

    RecordReference<PropertyRecord> previousProperty( long id );

    RecordReference<NeoStoreRecord> previousGraph();

    DynamicRecord changedSchema( long id );

    NodeRecord changedNode( long id );

    RelationshipRecord changedRelationship( long id );

    PropertyRecord changedProperty( long id );

    DynamicRecord changedString( long id );

    DynamicRecord changedArray( long id );

    RelationshipGroupRecord changedRelationshipGroup( long id );
}
