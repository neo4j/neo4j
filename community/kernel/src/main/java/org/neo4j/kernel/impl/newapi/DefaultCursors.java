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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.CursorFactory;

public class DefaultCursors implements CursorFactory
{
    private DefaultNodeCursor nodeCursor;
    private DefaultRelationshipScanCursor relationshipScanCursor;
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private DefaultPropertyCursor propertyCursor;
    private DefaultRelationshipGroupCursor relationshipGroupCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultNodeExplicitIndexCursor nodeExplicitIndexCursor;
    private DefaultRelationshipExplicitIndexCursor relationshipExplicitIndexCursor;

    @Override
    public DefaultNodeCursor allocateNodeCursor()
    {
        if ( nodeCursor == null )
        {
            return new DefaultNodeCursor( this );
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
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor()
    {
        if ( relationshipScanCursor == null )
        {
            return new DefaultRelationshipScanCursor( this );
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
            return new DefaultRelationshipTraversalCursor( new DefaultRelationshipGroupCursor( null ), this );
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
            return new DefaultPropertyCursor( this );
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
    public DefaultRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        if ( relationshipGroupCursor == null )
        {
            return new DefaultRelationshipGroupCursor( this );
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
            return new DefaultNodeValueIndexCursor( this );
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
            return new DefaultNodeLabelIndexCursor( this );
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
    public DefaultNodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        if ( nodeExplicitIndexCursor == null )
        {
            return new DefaultNodeExplicitIndexCursor( this );
        }

        try
        {
            return nodeExplicitIndexCursor;
        }
        finally
        {
            nodeExplicitIndexCursor = null;
        }
    }

    public void accept( DefaultNodeExplicitIndexCursor cursor )
    {
        if ( nodeExplicitIndexCursor != null )
        {
            nodeExplicitIndexCursor.release();
        }
        nodeExplicitIndexCursor = cursor;
    }

    @Override
    public DefaultRelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        if ( relationshipExplicitIndexCursor == null )
        {
            return new DefaultRelationshipExplicitIndexCursor( this );
        }

        try
        {
            return relationshipExplicitIndexCursor;
        }
        finally
        {
            relationshipExplicitIndexCursor = null;
        }
    }

    public void accept( DefaultRelationshipExplicitIndexCursor cursor )
    {
        if ( relationshipExplicitIndexCursor != null )
        {
            relationshipExplicitIndexCursor.release();
        }
        relationshipExplicitIndexCursor = cursor;
    }

    public void release()
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
            nodeCursor = null;
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
        if ( nodeExplicitIndexCursor != null )
        {
            nodeExplicitIndexCursor.release();
            nodeExplicitIndexCursor = null;
        }
        if ( relationshipExplicitIndexCursor != null )
        {
            relationshipExplicitIndexCursor.release();
            relationshipExplicitIndexCursor = null;
        }
    }
}
