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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

public enum DynamicStore
{
    SCHEMA( RecordType.SCHEMA )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.schema( block );
        }
    },
    STRING( RecordType.STRING_PROPERTY )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.string( block );
        }
    },
    ARRAY( RecordType.ARRAY_PROPERTY )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.array( block );
        }
    },
    PROPERTY_KEY( RecordType.PROPERTY_KEY_NAME )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.propertyKeyName( (int) block );
        }
    },
    RELATIONSHIP_TYPE( RecordType.RELATIONSHIP_TYPE_NAME )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.relationshipTypeName( (int) block );
        }
    },
    LABEL( RecordType.LABEL_NAME )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.labelName( (int) block );
        }
    },
    NODE_LABEL( RecordType.NODE_DYNAMIC_LABEL )
    {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.nodeLabels( block );
        }
    };

    public final RecordType type;

    private DynamicStore( RecordType type )
    {
        this.type = type;
    }

    abstract RecordReference<DynamicRecord> lookup(RecordAccess records, long block);
}
