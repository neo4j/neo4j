package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;
import java.util.LinkedList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.kernel.Traversal;

public class TraversalPath implements Path
{
    private final TraversalBranch branch;
    private LinkedList<Node> nodes;
    private LinkedList<Relationship> relationships;

    TraversalPath( TraversalBranch branch )
    {
        this.branch = branch;
    }
    
    private void ensureEntitiesAreGathered()
    {
        if ( nodes == null )
        {
            // We don't synchronize on nodes/relationship... and that's fine
            // because even if there would be a situation where two (or more)
            // threads comes here at the same time everything would still
            // work as expected (in here as well as outside).
            LinkedList<Node> nodesList = new LinkedList<Node>();
            LinkedList<Relationship> relationshipsList = new LinkedList<Relationship>();
            TraversalBranch stepper = branch;
            while ( stepper != null )
            {
                nodesList.addFirst( stepper.node() );
                Relationship relationship = stepper.relationship();
                if (relationship != null)
                {
                    relationshipsList.addFirst( relationship );
                }
                stepper = stepper.parent();
            }
            nodes = nodesList;
            relationships = relationshipsList;
        }
    }

    public Node startNode()
    {
        ensureEntitiesAreGathered();
        return nodes.getFirst();
    }

    public Node endNode()
    {
        return branch.node();
    }

    public Relationship lastRelationship()
    {
        return branch.relationship();
    }

    public Iterable<Node> nodes()
    {
        ensureEntitiesAreGathered();
        return nodes;
    }

    public Iterable<Relationship> relationships()
    {
        ensureEntitiesAreGathered();
        return relationships;
    }

    public Iterator<PropertyContainer> iterator()
    {
        ensureEntitiesAreGathered();
        return new Iterator<PropertyContainer>()
        {
            Iterator<? extends PropertyContainer> current = nodes().iterator();
            Iterator<? extends PropertyContainer> next = relationships().iterator();

            public boolean hasNext()
            {
                return current.hasNext();
            }

            public PropertyContainer next()
            {
                try
                {
                    return current.next();
                }
                finally
                {
                    Iterator<? extends PropertyContainer> temp = current;
                    current = next;
                    next = temp;
                }
            }

            public void remove()
            {
                next.remove();
            }
        };
    }

    public int length()
    {
        return branch.depth();
    }

    @Override
    public String toString()
    {
        return Traversal.defaultPathToString( this );
    }

    @Override
    public int hashCode()
    {
        ensureEntitiesAreGathered();
        if ( relationships.isEmpty() )
        {
            return startNode().hashCode();
        }
        else
        {
            return relationships.hashCode();
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        ensureEntitiesAreGathered();
        if ( this == obj )
        {
            return true;
        }
        else if ( obj instanceof TraversalPath )
        {
            TraversalPath other = (TraversalPath) obj;
            return startNode().equals( other.startNode() )
                   && relationships.equals( other.relationships );
        }
        else if ( obj instanceof Path )
        {
            Path other = (Path) obj;
            if ( startNode().equals( other.startNode() ) )
            {
                Iterator<Relationship> these = relationships().iterator();
                Iterator<Relationship> those = other.relationships().iterator();
                while ( these.hasNext() && those.hasNext() )
                {
                    if ( !these.next().equals( those.next() ) )
                    {
                        return false;
                    }
                }
                if ( these.hasNext() || those.hasNext() )
                {
                    return false;
                }
                return true;
            }
        }
        return false;
    }
}
