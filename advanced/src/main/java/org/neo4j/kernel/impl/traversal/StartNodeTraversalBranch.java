package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.kernel.impl.traversal.TraverserImpl.TraverserIterator;

class StartNodeTraversalBranch extends TraversalBranchImpl
{
    StartNodeTraversalBranch( TraverserIterator traverser, Node source,
            RelationshipExpander expander )
    {
        super( traverser, source, expander );
    }

    @Override
    public TraversalBranch next()
    {
        if ( !hasExpandedRelationships() )
        {
            if ( traverser.okToProceedFirst( this ) )
            {
                expandRelationships( false );
                return this;
            }
            else
            {
                return null;
            }
        }
        return super.next();
    }
}
