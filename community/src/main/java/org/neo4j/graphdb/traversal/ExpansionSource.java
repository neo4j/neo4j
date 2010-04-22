package org.neo4j.graphdb.traversal;

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;

class ExpansionSource
{
    final ExpansionSource parent;
    private final Node source;
    private final RelationshipExpander expander;
    private Iterator<Relationship> relationships;
    private final Relationship howIGotHere;
    private Position position;
    private final int depth;

    ExpansionSource( Traversal traversal, ExpansionSource parent, Node source,
            RelationshipExpander expander, Relationship toHere )
    {
        this.parent = parent;
        this.source = source;
        this.expander = expander;
        this.howIGotHere = toHere;
        if ( parent == null )
        {
            // We're at the start node
            depth = 0;
        }
        else
        {
            depth = parent.depth + 1;
            expandRelationships( traversal, true );
        }
    }

    private void expandRelationships( Traversal traversal, boolean doChecks )
    {
        boolean okToExpand = !doChecks || traversal.shouldExpandBeyond( this );
        relationships = okToExpand ?
                expander.expand( source ).iterator() :
                Collections.<Relationship>emptyList().iterator();
    }

    ExpansionSource next( Traversal traversal )
    {
        if ( relationships == null ) // This code will only be executed at the
                                     // start node
        {
            if ( traversal.uniquness.type == PrimitiveTypeFetcher.RELATIONSHIP
                 || traversal.okToProceed( this ) )
            {
                expandRelationships( traversal, false );
                return this;
            }
            else
            {
                return null;
            }
        }
        while ( relationships.hasNext() )
        {
            Relationship relationship = relationships.next();
            if ( relationship.equals( howIGotHere ) )
            {
                continue;
            }
            Node node = relationship.getOtherNode( source );
            ExpansionSource next = new ExpansionSource( traversal, this, node,
                    expander, relationship );
            if ( traversal.okToProceed( next ) )
            {
                return next;
            }
        }
        return null;
    }

    Position position( Traversal traversal )
    {
        return traversal.okToReturn( this ) ? position() : null;
    }

    Position position()
    {
        if ( this.position == null )
        {
            this.position = new PositionImpl( this );
        }
        return this.position;
    }

    int depth()
    {
        return depth;
    }

    Relationship relationship()
    {
        return howIGotHere;
    }

    Node node()
    {
        return source;
    }
}
