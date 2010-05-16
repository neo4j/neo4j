package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.kernel.impl.traversal.TraverserImpl.TraverserIterator;

class StartNodeExpansionSource extends ExpansionSourceImpl
{
    StartNodeExpansionSource( TraverserIterator traverser, Node source,
            RelationshipExpander expander )
    {
        super( traverser, source, expander );
        System.out.println( "START" );
    }

    @Override
    public ExpansionSource next()
    {
        if ( !hasExpandedRelationships() )
        {
            if ( traverser.uniquness.type == PrimitiveTypeFetcher.RELATIONSHIP
                 || traverser.okToProceed( this ) )
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
