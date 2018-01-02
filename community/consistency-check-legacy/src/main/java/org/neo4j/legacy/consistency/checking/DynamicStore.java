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
package org.neo4j.legacy.consistency.checking;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.legacy.consistency.RecordType;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;

public enum DynamicStore
{
    SCHEMA( RecordType.SCHEMA )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.schema( block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return records.changedSchema( id );
        }
    },
    STRING( RecordType.STRING_PROPERTY )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.string( block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return records.changedString( id );
        }
    },
    ARRAY( RecordType.ARRAY_PROPERTY )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.array( block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return records.changedArray( id );
        }
    },
    PROPERTY_KEY( RecordType.PROPERTY_KEY_NAME )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.propertyKeyName( (int) block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return null; // never needed
        }
    },
    RELATIONSHIP_TYPE( RecordType.RELATIONSHIP_TYPE_NAME )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.relationshipTypeName( (int) block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return null; // never needed
        }
    },
    LABEL( RecordType.LABEL_NAME )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.labelName( (int) block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return null; // never needed
        }
    },
    NODE_LABEL( RecordType.NODE_DYNAMIC_LABEL )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.nodeLabels( block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return null; // never needed (?)
        }
    };

    public final RecordType type;

    private DynamicStore( RecordType type )
    {
        this.type = type;
    }

    abstract RecordReference<DynamicRecord> lookup(RecordAccess records, long block);

    abstract DynamicRecord changed( DiffRecordAccess records, long id );
}
