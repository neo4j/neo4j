/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.ArrayList;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Cursor factory which pools 1 cursor of each kind. Not thread-safe at all.
 */
public class DefaultPooledCursors extends DefaultCursors implements CursorFactory
{
    private final StorageReader storageReader;
    private DefaultNodeCursor nodeCursor;
    private FullAccessNodeCursor fullAccessNodeCursor;
    private DefaultRelationshipScanCursor relationshipScanCursor;
    private FullAccessRelationshipScanCursor fullAccessRelationshipScanCursor;
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private FullAccessRelationshipTraversalCursor fullAccessRelationshipTraversalCursor;
    private DefaultPropertyCursor propertyCursor;
    private FullAccessPropertyCursor fullAccessPropertyCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private FullAccessNodeValueIndexCursor fullAccessNodeValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultNodeLabelIndexCursor fullAccessNodeLabelIndexCursor;
    private DefaultRelationshipValueIndexCursor relationshipValueIndexCursor;
    private DefaultRelationshipTypeIndexCursor relationshipTypeIndexCursor;

    public DefaultPooledCursors( StorageReader storageReader, Config config )
    {
        super( new ArrayList<>(), config );
        this.storageReader = storageReader;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor( PageCursorTracer cursorTracer )
    {
        if ( nodeCursor == null )
        {
            return trace( new DefaultNodeCursor( this::accept,
                    storageReader.allocateNodeCursor( cursorTracer ), storageReader.allocateNodeCursor( cursorTracer ) ) );
        }

        try
        {
            return acquire( nodeCursor );
        }
        finally
        {
            nodeCursor = null;
        }
    }

    private void accept( DefaultNodeCursor cursor )
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
        }
        cursor.removeTracer();
        nodeCursor = cursor;
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor( PageCursorTracer cursorTracer )
    {
        if ( fullAccessNodeCursor == null )
        {
            return trace( new FullAccessNodeCursor( this::acceptFullAccess, storageReader.allocateNodeCursor( cursorTracer ) ) );
        }

        try
        {
            return acquire( fullAccessNodeCursor );
        }
        finally
        {
            fullAccessNodeCursor = null;
        }
    }

    private void acceptFullAccess( DefaultNodeCursor cursor )
    {
        if ( fullAccessNodeCursor != null )
        {
            fullAccessNodeCursor.release();
        }
        cursor.removeTracer();
        fullAccessNodeCursor = (FullAccessNodeCursor) cursor;
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        if ( relationshipScanCursor == null )
        {
            return trace( new DefaultRelationshipScanCursor( this::accept, storageReader.allocateRelationshipScanCursor( cursorTracer ),
                    new DefaultNodeCursor( this::accept,
                                           storageReader.allocateNodeCursor( cursorTracer ), storageReader.allocateNodeCursor( cursorTracer ) ) ) );
        }

        try
        {
            return acquire( relationshipScanCursor );
        }
        finally
        {
            relationshipScanCursor = null;
        }
    }

    private void accept( DefaultRelationshipScanCursor cursor )
    {
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.release();
        }
        cursor.removeTracer();
        relationshipScanCursor = cursor;
    }

    @Override
    public RelationshipScanCursor allocateFullAccessRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        if ( fullAccessRelationshipScanCursor == null )
        {
            return trace( new FullAccessRelationshipScanCursor( this::acceptFullAccess, storageReader.allocateRelationshipScanCursor( cursorTracer ) ) );
        }

        try
        {
            return acquire( fullAccessRelationshipScanCursor );
        }
        finally
        {
            fullAccessRelationshipScanCursor = null;
        }
    }

    private <C extends TraceableCursor<?>> C acquire( C cursor )
    {
        cursor.acquire();
        return cursor;
    }

    private void acceptFullAccess( DefaultRelationshipScanCursor cursor )
    {
        if ( fullAccessRelationshipScanCursor != null )
        {
            fullAccessRelationshipScanCursor.release();
        }
        cursor.removeTracer();
        fullAccessRelationshipScanCursor = (FullAccessRelationshipScanCursor) cursor;
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        if ( relationshipTraversalCursor == null )
        {
            return trace( new DefaultRelationshipTraversalCursor( this::accept, storageReader.allocateRelationshipTraversalCursor( cursorTracer ),
                    new DefaultNodeCursor( this::accept,
                                           storageReader.allocateNodeCursor( cursorTracer ), storageReader.allocateNodeCursor( cursorTracer ) ) ) );
        }

        try
        {
            return acquire( relationshipTraversalCursor );
        }
        finally
        {
            relationshipTraversalCursor = null;
        }
    }

    void accept( DefaultRelationshipTraversalCursor cursor )
    {
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
        }
        cursor.removeTracer();
        relationshipTraversalCursor = cursor;
    }

    @Override
    public RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        if ( fullAccessRelationshipTraversalCursor == null )
        {
            return trace( new FullAccessRelationshipTraversalCursor( this::acceptFullAccess,
                                                                     storageReader.allocateRelationshipTraversalCursor( cursorTracer ) ) );
        }

        try
        {
            return acquire( fullAccessRelationshipTraversalCursor );
        }
        finally
        {
            fullAccessRelationshipTraversalCursor = null;
        }
    }

    private void acceptFullAccess( DefaultRelationshipTraversalCursor cursor )
    {
        if ( fullAccessRelationshipTraversalCursor != null )
        {
            fullAccessRelationshipTraversalCursor.release();
        }
        cursor.removeTracer();
        fullAccessRelationshipTraversalCursor = (FullAccessRelationshipTraversalCursor) cursor;
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( propertyCursor == null )
        {
            FullAccessNodeCursor nodeCursor = new FullAccessNodeCursor( this::acceptFullAccess, storageReader.allocateNodeCursor( cursorTracer ) );
            FullAccessRelationshipScanCursor relCursor = new FullAccessRelationshipScanCursor(
                    this::acceptFullAccess, storageReader.allocateRelationshipScanCursor( cursorTracer ) );
            return trace( new DefaultPropertyCursor( this::accept, storageReader.allocatePropertyCursor( cursorTracer, memoryTracker ), nodeCursor,
                    relCursor ) );
        }

        try
        {
            return acquire( propertyCursor );
        }
        finally
        {
            propertyCursor = null;
        }
    }

    private void accept( DefaultPropertyCursor cursor )
    {
        if ( propertyCursor != null )
        {
            propertyCursor.release();
        }
        cursor.removeTracer();
        propertyCursor = cursor;
    }

    @Override
    public FullAccessPropertyCursor allocateFullAccessPropertyCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( fullAccessPropertyCursor == null )
        {
            return trace( new FullAccessPropertyCursor( this::acceptFullAccess, storageReader.allocatePropertyCursor( cursorTracer, memoryTracker ) ) );
        }

        try
        {
            return acquire( fullAccessPropertyCursor );
        }
        finally
        {
            fullAccessPropertyCursor = null;
        }
    }

    private void acceptFullAccess( DefaultPropertyCursor cursor )
    {
        if ( fullAccessPropertyCursor != null )
        {
            fullAccessPropertyCursor.release();
        }
        cursor.removeTracer();
        fullAccessPropertyCursor = (FullAccessPropertyCursor) cursor;
    }

    @Override
    public DefaultNodeValueIndexCursor allocateNodeValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( nodeValueIndexCursor == null )
        {
            return trace( new DefaultNodeValueIndexCursor( this::accept,
                    new DefaultNodeCursor( this::accept,
                            storageReader.allocateNodeCursor( cursorTracer ), storageReader.allocateNodeCursor( cursorTracer ) ), memoryTracker ) );
        }

        try
        {
            return acquire( nodeValueIndexCursor );
        }
        finally
        {
            nodeValueIndexCursor = null;
        }
    }

    private void accept( DefaultNodeValueIndexCursor cursor )
    {
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
        }
        cursor.removeTracer();
        nodeValueIndexCursor = cursor;
    }

    @Override
    public FullAccessNodeValueIndexCursor allocateFullAccessNodeValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( fullAccessNodeValueIndexCursor == null )
        {
            return trace( new FullAccessNodeValueIndexCursor( this::acceptFullAccess, memoryTracker ) );
        }

        try
        {
            return acquire( fullAccessNodeValueIndexCursor );
        }
        finally
        {
            fullAccessNodeValueIndexCursor = null;
        }
    }

    private void acceptFullAccess( DefaultNodeValueIndexCursor cursor )
    {
        if ( fullAccessNodeValueIndexCursor != null )
        {
            fullAccessNodeValueIndexCursor.release();
        }
        cursor.removeTracer();
        fullAccessNodeValueIndexCursor = (FullAccessNodeValueIndexCursor) cursor;
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor( PageCursorTracer cursorTracer )
    {
        if ( nodeLabelIndexCursor == null )
        {
            return trace( new DefaultNodeLabelIndexCursor( this::accept, new DefaultNodeCursor(
                    this::accept, storageReader.allocateNodeCursor( cursorTracer ), storageReader.allocateNodeCursor( cursorTracer ) ) ) );
        }

        try
        {
            return acquire( nodeLabelIndexCursor );
        }
        finally
        {
            nodeLabelIndexCursor = null;
        }
    }

    private void accept( DefaultNodeLabelIndexCursor cursor )
    {
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
        }
        cursor.removeTracer();
        nodeLabelIndexCursor = cursor;
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor( PageCursorTracer cursorTracer )
    {
        if ( fullAccessNodeLabelIndexCursor == null )
        {
            return trace( new FullAccessNodeLabelIndexCursor( this::acceptFullAccess ) );
        }

        try
        {
            return acquire( fullAccessNodeLabelIndexCursor );
        }
        finally
        {
            fullAccessNodeLabelIndexCursor = null;
        }
    }

    private void acceptFullAccess( DefaultNodeLabelIndexCursor cursor )
    {
        if ( fullAccessNodeLabelIndexCursor != null )
        {
            fullAccessNodeLabelIndexCursor.release();
        }
        fullAccessNodeLabelIndexCursor = cursor;
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( relationshipValueIndexCursor == null )
        {
            DefaultNodeCursor nodeCursor = new DefaultNodeCursor( this::accept,
                    storageReader.allocateNodeCursor( cursorTracer ), storageReader.allocateNodeCursor( cursorTracer ) );
            DefaultRelationshipScanCursor relationshipScanCursor = new DefaultRelationshipScanCursor(
                    this::accept, storageReader.allocateRelationshipScanCursor( cursorTracer ), nodeCursor );
            return trace( new DefaultRelationshipValueIndexCursor( this::accept, relationshipScanCursor, memoryTracker ) );
        }

        try
        {
            return acquire( relationshipValueIndexCursor );
        }
        finally
        {
            relationshipValueIndexCursor = null;
        }
    }

    public void accept( DefaultRelationshipValueIndexCursor cursor )
    {
        if ( relationshipValueIndexCursor != null )
        {
            relationshipValueIndexCursor.release();
        }
        cursor.removeTracer();
        relationshipValueIndexCursor = cursor;
    }

    @Override
    public DefaultRelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor()
    {
        if ( relationshipTypeIndexCursor == null )
        {
            return trace( new DefaultRelationshipTypeIndexCursor( this::accept ) );
        }

        try
        {
            return acquire( relationshipTypeIndexCursor );
        }
        finally
        {
            relationshipTypeIndexCursor = null;
        }
    }

    private void accept( DefaultRelationshipTypeIndexCursor cursor )
    {
        if ( relationshipTypeIndexCursor != null )
        {
            relationshipTypeIndexCursor.release();
        }
        cursor.removeTracer();
        relationshipTypeIndexCursor = cursor;
    }

    public void release()
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
            nodeCursor = null;
        }
        if ( fullAccessNodeCursor != null )
        {
            fullAccessNodeCursor.release();
            fullAccessNodeCursor = null;
        }
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.release();
            relationshipScanCursor = null;
        }
        if ( fullAccessRelationshipScanCursor != null )
        {
            fullAccessRelationshipScanCursor.release();
            fullAccessRelationshipScanCursor = null;
        }
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
            relationshipTraversalCursor = null;
        }
        if ( fullAccessRelationshipTraversalCursor != null )
        {
            fullAccessRelationshipTraversalCursor.release();
            fullAccessRelationshipTraversalCursor = null;
        }
        if ( propertyCursor != null )
        {
            propertyCursor.release();
            propertyCursor = null;
        }
        if ( fullAccessPropertyCursor != null )
        {
            fullAccessPropertyCursor.release();
            fullAccessPropertyCursor = null;
        }
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
            nodeValueIndexCursor = null;
        }
        if ( fullAccessNodeValueIndexCursor != null )
        {
            fullAccessNodeValueIndexCursor.release();
            fullAccessNodeValueIndexCursor = null;
        }
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
            nodeLabelIndexCursor = null;
        }
        if ( fullAccessNodeLabelIndexCursor != null )
        {
            fullAccessNodeLabelIndexCursor.release();
            fullAccessNodeLabelIndexCursor = null;
        }
        if ( relationshipValueIndexCursor != null )
        {
            relationshipValueIndexCursor.release();
            relationshipValueIndexCursor = null;
        }
        if ( relationshipTypeIndexCursor != null )
        {
            relationshipTypeIndexCursor.release();
            relationshipTypeIndexCursor = null;
        }
    }
}
