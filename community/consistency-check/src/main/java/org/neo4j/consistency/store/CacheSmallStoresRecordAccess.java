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

import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public class CacheSmallStoresRecordAccess extends DelegatingRecordAccess
{
    private final PropertyKeyTokenRecord[] propertyKeys;
    private final RelationshipTypeTokenRecord[] relationshipTypes;
    private final LabelTokenRecord[] labels;

    public CacheSmallStoresRecordAccess( RecordAccess delegate,
                                         PropertyKeyTokenRecord[] propertyKeys,
                                         RelationshipTypeTokenRecord[] relationshipTypes,
                                         LabelTokenRecord[] labels )
    {
        super(delegate);
        this.propertyKeys = propertyKeys;
        this.relationshipTypes = relationshipTypes;
        this.labels = labels;
    }

    @Override
    public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id )
    {
        if ( id < relationshipTypes.length )
        {
            return new DirectRecordReference<>( relationshipTypes[id], this );
        }
        else
        {
            return super.relationshipType( id );
        }
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
    {
        if ( id < propertyKeys.length )
        {
            return new DirectRecordReference<>( propertyKeys[id], this );
        }
        else
        {
            return super.propertyKey( id );
        }
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id )
    {
        if ( id < labels.length )
        {
            return new DirectRecordReference<>( labels[id], this );
        }
        else
        {
            return super.label( id );
        }
    }
}
