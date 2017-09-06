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

class Cursors implements org.neo4j.internal.kernel.api.CursorFactory
{
    private final Read read;

    Cursors( Read read )
    {
        this.read = read;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return new NodeCursor( read );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new RelationshipScanCursor( read );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new RelationshipTraversalCursor( read );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return new PropertyCursor( read );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new RelationshipGroupCursor( read );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return new NodeValueIndexCursor( read );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return new NodeLabelIndexCursor( read );
    }

    @Override
    public NodeManualIndexCursor allocateNodeManualIndexCursor()
    {
        return new NodeManualIndexCursor( read );
    }

    @Override
    public RelationshipManualIndexCursor allocateRelationshipManualIndexCursor()
    {
        return new RelationshipManualIndexCursor( read );
    }
}
