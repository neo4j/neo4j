/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

public class Cursors implements org.neo4j.internal.kernel.api.CursorFactory
{
    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new NodeCursor( );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new RelationshipScanCursor( );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new RelationshipTraversalCursor( allocateRelationshipGroupCursor() );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new PropertyCursor( );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new RelationshipGroupCursor( );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return new NodeValueIndexCursor( );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return new NodeLabelIndexCursor( );
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return new NodeExplicitIndexCursor( );
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return new RelationshipExplicitIndexCursor( );
    }
}
