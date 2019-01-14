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

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageReader;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

abstract class StoreScanChunk<T extends StorageEntityCursor> implements InputChunk
{
    protected final StoragePropertyCursor storePropertyCursor;
    protected final T cursor;
    private final boolean requiresPropertyMigration;
    private long id;
    private long endId;

    StoreScanChunk( T cursor, RecordStorageReader storageReader, boolean requiresPropertyMigration )
    {
        this.cursor = cursor;
        this.requiresPropertyMigration = requiresPropertyMigration;
        this.storePropertyCursor = storageReader.allocatePropertyCursor();
    }

    void visitProperties( T record, InputEntityVisitor visitor )
    {
        if ( !requiresPropertyMigration )
        {
            visitor.propertyId( record.propertiesReference() );
        }
        else
        {
            storePropertyCursor.init( record.propertiesReference() );
            while ( storePropertyCursor.next() )
            {
                // add key as int here as to have the importer use the token id
                visitor.property( storePropertyCursor.propertyKey(), storePropertyCursor.propertyValue().asObject() );
            }
            storePropertyCursor.close();
        }
    }

    @Override
    public void close()
    {
        storePropertyCursor.close();
    }

    @Override
    public boolean next( InputEntityVisitor visitor ) throws IOException
    {
        if ( id < endId )
        {
            read( cursor, id );
            if ( cursor.next() )
            {
                visitRecord( cursor, visitor );
                visitor.endOfEntity();
            }
            id++;
            return true;
        }
        return false;
    }

    protected abstract void read( T cursor, long id );

    public void initialize( long startId, long endId )
    {
        this.id = startId;
        this.endId = endId;
    }

    abstract void visitRecord( T record, InputEntityVisitor visitor );
}
