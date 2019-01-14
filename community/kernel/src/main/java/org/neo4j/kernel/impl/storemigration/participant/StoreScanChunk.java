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
package org.neo4j.kernel.impl.storemigration.participant;

import java.io.IOException;

import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.impl.api.store.StorePropertyCursor;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

abstract class StoreScanChunk<T extends PrimitiveRecord> implements InputChunk
{
    protected final StorePropertyCursor storePropertyCursor;
    protected final RecordCursors recordCursors;
    private final RecordCursor<T> cursor;
    private final boolean requiresPropertyMigration;
    private long id;
    private long endId;

    StoreScanChunk( RecordCursor<T> cursor, NeoStores neoStores, boolean requiresPropertyMigration )
    {
        this.cursor = cursor;
        this.recordCursors = new RecordCursors( neoStores );
        this.requiresPropertyMigration = requiresPropertyMigration;
        this.storePropertyCursor = new StorePropertyCursor( recordCursors, ignored -> {} );
    }

    void visitProperties( T record, InputEntityVisitor visitor )
    {
        if ( !requiresPropertyMigration )
        {
            visitor.propertyId( record.getNextProp() );
        }
        else
        {
            storePropertyCursor.init( record.getNextProp(), LockService.NO_LOCK, AssertOpen.ALWAYS_OPEN );
            while ( storePropertyCursor.next() )
            {
                // add key as int here as to have the importer use the token id
                visitor.property( storePropertyCursor.propertyKeyId(), storePropertyCursor.value().asObject() );
            }
            storePropertyCursor.close();
        }
    }

    @Override
    public void close()
    {
        recordCursors.close();
        cursor.close();
    }

    @Override
    public boolean next( InputEntityVisitor visitor ) throws IOException
    {
        if ( id < endId )
        {
            if ( cursor.next( id ) )
            {
                visitRecord( cursor.get(), visitor );
                visitor.endOfEntity();
            }
            id++;
            return true;
        }
        return false;
    }

    public void initialize( long startId, long endId )
    {
        this.id = startId;
        this.endId = endId;
    }

    abstract void visitRecord( T record, InputEntityVisitor visitor );
}
