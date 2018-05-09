/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageReader;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

abstract class StoreScanChunk<T extends Cursor> implements InputChunk
{
    final T cursor;
    final PropertyCursor propertyCursor;
    final RecordStorageReader storageReader;
    private final boolean requiresPropertyMigration;
    private long id;
    private long endId;

    StoreScanChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration, T cursor )
    {
        this.storageReader = storageReader;
        this.cursor = cursor;
        this.requiresPropertyMigration = requiresPropertyMigration;
        this.propertyCursor = storageReader.allocatePropertyCursor();
    }

    void visitProperties( InputEntityVisitor visitor )
    {
        if ( !requiresPropertyMigration )
        {
            visitor.propertyId( propertyId() );
        }
        else
        {
            initializePropertyCursor();
            while ( propertyCursor.next() )
            {
                // add key as int here as to have the importer use the token id
                visitor.property( propertyCursor.propertyKey(), propertyCursor.propertyValue().asObject() );
            }
            propertyCursor.close();
        }
    }

    protected abstract long propertyId();

    protected abstract void initializePropertyCursor();

    @Override
    public void close()
    {
        propertyCursor.close();
    }

    @Override
    public boolean next( InputEntityVisitor visitor ) throws IOException
    {
        if ( id < endId )
        {
            positionCursor( id );
            if ( cursor.next() )
            {
                visitRecord( visitor );
                visitor.endOfEntity();
            }
            id++;
            return true;
        }
        return false;
    }

    protected abstract void positionCursor( long id );

    public void initialize( long startId, long endId )
    {
        this.id = startId;
        this.endId = endId;
    }

    abstract void visitRecord( InputEntityVisitor visitor );

    abstract static class NodeStoreScanChunk extends StoreScanChunk<NodeCursor>
    {
        NodeStoreScanChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration )
        {
            super( storageReader, requiresPropertyMigration, storageReader.allocateNodeCursor() );
        }

        @Override
        protected long propertyId()
        {
            return cursor.propertiesReference();
        }

        @Override
        protected void initializePropertyCursor()
        {
            cursor.properties( propertyCursor );
        }

        @Override
        protected void positionCursor( long id )
        {
            storageReader.singleNode( id, cursor );
        }
    }

    abstract static class RelationshipStoreScanChunk extends StoreScanChunk<RelationshipScanCursor>
    {
        RelationshipStoreScanChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration )
        {
            super( storageReader, requiresPropertyMigration, storageReader.allocateRelationshipScanCursor() );
        }

        @Override
        protected long propertyId()
        {
            return cursor.propertiesReference();
        }

        @Override
        protected void initializePropertyCursor()
        {
            cursor.properties( propertyCursor );
        }

        @Override
        protected void positionCursor( long id )
        {
            storageReader.singleRelationship( id, cursor );
        }
    }
}
