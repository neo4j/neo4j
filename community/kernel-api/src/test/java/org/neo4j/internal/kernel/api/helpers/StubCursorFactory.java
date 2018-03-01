package org.neo4j.internal.kernel.api.helpers;

import java.util.Arrays;
import java.util.Deque;
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
    private Queue<NodeCursor> nodeCursors = new LinkedList<>(  );
    private Queue<RelationshipScanCursor> relationshipScanCursors = new LinkedList<>(  );
    private Queue<RelationshipTraversalCursor> relationshiTraversalCursors = new LinkedList<>(  );
    private Queue<PropertyCursor> propertyCursors = new LinkedList<>(  );
    private Queue<RelationshipGroupCursor> groupCursors = new LinkedList<>(  );
    private Queue<NodeValueIndexCursor> nodeValueIndexCursors = new LinkedList<>(  );
    private Queue<NodeLabelIndexCursor> nodeLabelIndexCursors = new LinkedList<>(  );
    private Queue<NodeExplicitIndexCursor> nodeExplicitIndexCursors = new LinkedList<>(  );
    private Queue<RelationshipExplicitIndexCursor> relationshipExplicitIndexCursors = new LinkedList<>(  );

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return nodeCursors.poll();
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return relationshipScanCursors.poll();
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return relationshiTraversalCursors.poll();
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return propertyCursors.poll();
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return groupCursors.poll();
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return nodeValueIndexCursors.poll();
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return nodeLabelIndexCursors.poll();
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return nodeExplicitIndexCursors.poll();
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return relationshipExplicitIndexCursors.poll();
    }

    StubCursorFactory withGroupCursors( RelationshipGroupCursor...cursors )
    {
        groupCursors.addAll( Arrays.asList( cursors ) );
        return this;
    }
}
