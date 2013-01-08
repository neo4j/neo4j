/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.consistency.checking.old;

import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .PROPERTY_NOT_REMOVED_FOR_DELETED_NODE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

@Deprecated
abstract class PropertyOwner
{
    final long id;

    PropertyOwner( long id )
    {
        this.id = id;
    }

    abstract RecordStore<? extends PrimitiveRecord> storeFrom( RecordStore<NodeRecord> nodes, RecordStore<RelationshipRecord> rels );

    abstract long otherOwnerOf( PropertyRecord prop );

    abstract long ownerOf( PropertyRecord prop );

    abstract InconsistencyType propertyNotRemoved();

    public static final class OwningNode extends PropertyOwner
    {
        OwningNode( long id )
        {
            super( id );
        }

        @Override
        RecordStore<? extends PrimitiveRecord> storeFrom( RecordStore<NodeRecord> nodes, RecordStore<RelationshipRecord> rels )
        {
            return nodes;
        }

        @Override
        InconsistencyType propertyNotRemoved()
        {
            return PROPERTY_NOT_REMOVED_FOR_DELETED_NODE;
        }

        @Override
        long otherOwnerOf( PropertyRecord prop )
        {
            return prop.getRelId();
        }

        @Override
        long ownerOf( PropertyRecord prop )
        {
            return prop.getNodeId();
        }
    }

    public static final class OwningRelationship extends PropertyOwner
    {
        OwningRelationship( long id )
        {
            super( id );
        }

        @Override
        RecordStore<? extends PrimitiveRecord> storeFrom( RecordStore<NodeRecord> nodes, RecordStore<RelationshipRecord> rels )
        {
            return rels;
        }

        @Override
        InconsistencyType propertyNotRemoved()
        {
            return PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP;
        }

        @Override
        long otherOwnerOf( PropertyRecord prop )
        {
            return prop.getNodeId();
        }

        @Override
        long ownerOf( PropertyRecord prop )
        {
            return prop.getRelId();
        }
    }
}
