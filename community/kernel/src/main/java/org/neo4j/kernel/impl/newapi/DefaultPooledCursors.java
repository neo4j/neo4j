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
package org.neo4j.kernel.impl.newapi;

import java.util.ArrayList;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
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
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private DefaultPropertyCursor propertyCursor;
    private FullAccessPropertyCursor fullAccessPropertyCursor;
    private DefaultRelationshipGroupCursor relationshipGroupCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultRelationshipIndexCursor relationshipIndexCursor;

    public DefaultPooledCursors( StorageReader storageReader )
    {
        super( new ArrayList<>() );
        this.storageReader = storageReader;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor()
    {
        if ( nodeCursor == null )
        {
            return trace( new DefaultNodeCursor( this::accept, storageReader.allocateNodeCursor() ) );
        }

        try
        {
            return nodeCursor;
        }
        finally
        {
            nodeCursor = null;
        }
    }

    public void accept( DefaultNodeCursor cursor )
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
        }
        nodeCursor = cursor;
    }

    @Override
    public FullAccessNodeCursor allocateFullAccessNodeCursor()
    {
        if ( fullAccessNodeCursor == null )
        {
            return trace( new FullAccessNodeCursor( this::acceptFullAccess, storageReader.allocateNodeCursor() ) );
        }

        try
        {
            return fullAccessNodeCursor;
        }
        finally
        {
            fullAccessNodeCursor = null;
        }
    }

    public void acceptFullAccess( DefaultNodeCursor cursor )
    {
        if ( fullAccessNodeCursor != null )
        {
            fullAccessNodeCursor.release();
        }
        fullAccessNodeCursor = (FullAccessNodeCursor) cursor;

    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor()
    {
        if ( relationshipScanCursor == null )
        {
            return trace( new DefaultRelationshipScanCursor( this::accept, storageReader.allocateRelationshipScanCursor() ) );
        }

        try
        {
            return relationshipScanCursor;
        }
        finally
        {
            relationshipScanCursor = null;
        }
    }

    public void accept( DefaultRelationshipScanCursor cursor )
    {
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.release();
        }
        relationshipScanCursor = cursor;
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        if ( relationshipTraversalCursor == null )
        {
            return trace( new DefaultRelationshipTraversalCursor( this::accept, storageReader.allocateRelationshipTraversalCursor() ) );
        }

        try
        {
            return relationshipTraversalCursor;
        }
        finally
        {
            relationshipTraversalCursor = null;
        }
    }

    public void accept( DefaultRelationshipTraversalCursor cursor )
    {
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
        }
        relationshipTraversalCursor = cursor;
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor()
    {
        if ( propertyCursor == null )
        {
            return trace( new DefaultPropertyCursor( this::accept, storageReader.allocatePropertyCursor() ) );
        }

        try
        {
            return propertyCursor;
        }
        finally
        {
            propertyCursor = null;
        }
    }

    public void accept( DefaultPropertyCursor cursor )
    {
        if ( propertyCursor != null )
        {
            propertyCursor.release();
        }
        propertyCursor = cursor;
    }

    @Override
    public FullAccessPropertyCursor allocateFullAccessPropertyCursor()
    {
        if ( fullAccessPropertyCursor == null )
        {
            return trace( new FullAccessPropertyCursor( this::acceptFullAccess, storageReader.allocatePropertyCursor() ) );
        }

        try
        {
            return fullAccessPropertyCursor;
        }
        finally
        {
            fullAccessPropertyCursor = null;
        }
    }

    public void acceptFullAccess( DefaultPropertyCursor cursor )
    {
        if ( fullAccessPropertyCursor != null )
        {
            fullAccessPropertyCursor.release();
        }
        fullAccessPropertyCursor = (FullAccessPropertyCursor) cursor;
    }

    @Override
    public DefaultRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        if ( relationshipGroupCursor == null )
        {
            return trace( new DefaultRelationshipGroupCursor( this::accept, storageReader.allocateRelationshipGroupCursor() ) );
        }

        try
        {
            return relationshipGroupCursor;
        }
        finally
        {
            relationshipGroupCursor = null;
        }
    }

    public void accept( DefaultRelationshipGroupCursor cursor )
    {
        if ( relationshipGroupCursor != null )
        {
            relationshipGroupCursor.release();
        }
        relationshipGroupCursor = cursor;
    }

    @Override
    public DefaultNodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        if ( nodeValueIndexCursor == null )
        {
            return trace( new DefaultNodeValueIndexCursor( this::accept ) );
        }

        try
        {
            return nodeValueIndexCursor;
        }
        finally
        {
            nodeValueIndexCursor = null;
        }
    }

    public void accept( DefaultNodeValueIndexCursor cursor )
    {
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
        }
        nodeValueIndexCursor = cursor;
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        if ( nodeLabelIndexCursor == null )
        {
            return trace( new DefaultNodeLabelIndexCursor( this::accept ) );
        }

        try
        {
            return nodeLabelIndexCursor;
        }
        finally
        {
            nodeLabelIndexCursor = null;
        }
    }

    public void accept( DefaultNodeLabelIndexCursor cursor )
    {
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
        }
        nodeLabelIndexCursor = cursor;
    }

    @Override
    public RelationshipIndexCursor allocateRelationshipIndexCursor()
    {
        if ( relationshipIndexCursor == null )
        {
            return trace( new DefaultRelationshipIndexCursor( this::accept ) );
        }

        try
        {
            return relationshipIndexCursor;
        }
        finally
        {
            relationshipIndexCursor = null;
        }
    }

    public void accept( DefaultRelationshipIndexCursor cursor )
    {
        if ( relationshipIndexCursor != null )
        {
            relationshipIndexCursor.release();
        }
        relationshipIndexCursor = cursor;
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
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
            relationshipTraversalCursor = null;
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
        if ( relationshipGroupCursor != null )
        {
            relationshipGroupCursor.release();
            relationshipGroupCursor = null;
        }
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
            nodeValueIndexCursor = null;
        }
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
            nodeLabelIndexCursor = null;
        }
    }
}
