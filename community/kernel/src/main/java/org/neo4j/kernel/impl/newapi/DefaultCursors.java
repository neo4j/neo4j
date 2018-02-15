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
import org.neo4j.kernel.impl.util.InstanceCache;

public class DefaultCursors implements CursorFactory
{
    private final InstanceCache<DefaultNodeCursor> nodeCursor;
    private final InstanceCache<DefaultRelationshipScanCursor> relationshipScanCursor;
    private final InstanceCache<DefaultRelationshipTraversalCursor> relationshipTraversalCursor;
    private final InstanceCache<DefaultPropertyCursor> propertyCursor;
    private final InstanceCache<DefaultRelationshipGroupCursor> relationshipGroupCursor;
    private final InstanceCache<DefaultNodeValueIndexCursor> nodeValueIndexCursor;
    private final InstanceCache<DefaultNodeLabelIndexCursor> nodeLabelIndexCursor;
    private final InstanceCache<DefaultNodeExplicitIndexCursor> nodeExplicitIndexCursor;
    private final InstanceCache<DefaultRelationshipExplicitIndexCursor> relationshipExplicitIndexCursor;

    public DefaultCursors()
    {
        nodeCursor = new InstanceCache<DefaultNodeCursor>()
        {
            @Override
            protected DefaultNodeCursor create()
            {
                return new DefaultNodeCursor( this );
            }
        };

        relationshipScanCursor = new InstanceCache<DefaultRelationshipScanCursor>()
        {
            @Override
            protected DefaultRelationshipScanCursor create()
            {
                return new DefaultRelationshipScanCursor( this );
            }
        };

        relationshipTraversalCursor = new InstanceCache<DefaultRelationshipTraversalCursor>()
        {
            @Override
            protected DefaultRelationshipTraversalCursor create()
            {
                return new DefaultRelationshipTraversalCursor( new DefaultRelationshipGroupCursor( cursor -> {} ), this );
            }
        };

        propertyCursor = new InstanceCache<DefaultPropertyCursor>()
        {
            @Override
            protected DefaultPropertyCursor create()
            {
                return new DefaultPropertyCursor( this );
            }
        };

        relationshipGroupCursor = new InstanceCache<DefaultRelationshipGroupCursor>()
        {
            @Override
            protected DefaultRelationshipGroupCursor create()
            {
                return new DefaultRelationshipGroupCursor( this );
            }
        };

        nodeValueIndexCursor = new InstanceCache<DefaultNodeValueIndexCursor>()
        {
            @Override
            protected DefaultNodeValueIndexCursor create()
            {
                return new DefaultNodeValueIndexCursor( this );
            }
        };

        nodeLabelIndexCursor = new InstanceCache<DefaultNodeLabelIndexCursor>()
        {
            @Override
            protected DefaultNodeLabelIndexCursor create()
            {
                return new DefaultNodeLabelIndexCursor( this );
            }
        };

        nodeExplicitIndexCursor = new InstanceCache<DefaultNodeExplicitIndexCursor>()
        {
            @Override
            protected DefaultNodeExplicitIndexCursor create()
            {
                return new DefaultNodeExplicitIndexCursor( this );
            }
        };

        relationshipExplicitIndexCursor = new InstanceCache<DefaultRelationshipExplicitIndexCursor>()
        {
            @Override
            protected DefaultRelationshipExplicitIndexCursor create()
            {
                return new DefaultRelationshipExplicitIndexCursor( this );
            }
        };
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor()
    {
        return nodeCursor.get();
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor()
    {
        return relationshipScanCursor.get( );
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return relationshipTraversalCursor.get();
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor()
    {
        return propertyCursor.get();
    }

    @Override
    public DefaultRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return relationshipGroupCursor.get();
    }

    @Override
    public DefaultNodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return nodeValueIndexCursor.get();
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return nodeLabelIndexCursor.get();
    }

    @Override
    public DefaultNodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return nodeExplicitIndexCursor.get();
    }

    @Override
    public DefaultRelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return relationshipExplicitIndexCursor.get();
    }
}
