/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.consistency.store.SkippingRecordAccess;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

class SkipAllButCached extends SkippingRecordAccess
{
    final DiffRecordAccess recordAccess;

    SkipAllButCached( DiffRecordAccess recordAccess )
    {
        this.recordAccess = recordAccess;
    }

    @Override
    public RecordReference<PropertyIndexRecord> propertyKey( int id )
    {
        return recordAccess.propertyKey( id );
    }

    @Override
    public RecordReference<RelationshipTypeRecord> relationshipLabel( int id )
    {
        return recordAccess.relationshipLabel( id );
    }
}
