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
package org.neo4j.internal.kernel.api.helpers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

public class StubCursorFactory implements CursorFactory
{
    private final boolean continueWithLastItem;
    private Queue<NodeCursor> nodeCursors = new LinkedList<>(  );
    private Queue<RelationshipScanCursor> relationshipScanCursors = new LinkedList<>(  );
    private Queue<RelationshipTraversalCursor> relationshiTraversalCursors = new LinkedList<>(  );
    private Queue<PropertyCursor> propertyCursors = new LinkedList<>(  );
    private Queue<RelationshipGroupCursor> groupCursors = new LinkedList<>(  );
    private Queue<NodeValueIndexCursor> nodeValueIndexCursors = new LinkedList<>(  );
    private Queue<NodeLabelIndexCursor> nodeLabelIndexCursors = new LinkedList<>(  );
    private Queue<NodeExplicitIndexCursor> nodeExplicitIndexCursors = new LinkedList<>(  );
    private Queue<RelationshipExplicitIndexCursor> relationshipExplicitIndexCursors = new LinkedList<>(  );

    public StubCursorFactory()
    {
        this( false );
    }

    public StubCursorFactory( boolean continueWithLastItem )
    {
        this.continueWithLastItem = continueWithLastItem;
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return poll( nodeCursors );
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return poll( relationshipScanCursors );
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return poll( relationshiTraversalCursors );
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return poll( propertyCursors );
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return poll( groupCursors );
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return poll( nodeValueIndexCursors );
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return poll( nodeLabelIndexCursors );
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return poll( nodeExplicitIndexCursors );
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return poll( relationshipExplicitIndexCursors );
    }

    public StubCursorFactory withGroupCursors( RelationshipGroupCursor...cursors )
    {
        groupCursors.addAll( Arrays.asList( cursors ) );
        return this;
    }

    public StubCursorFactory withRelationshipTraversalCursors( RelationshipTraversalCursor...cursors )
    {
        relationshiTraversalCursors.addAll( Arrays.asList( cursors ) );
        return this;
    }

    private <T> T poll( Queue<T> queue )
    {
        T poll = queue.poll();
        if ( continueWithLastItem && queue.isEmpty() )
        {
            queue.offer( poll );
        }
        return poll;
    }
}
