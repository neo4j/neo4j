package org.neo4j.kernel.impl.traversal;

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;

class ExpansionSourceImpl implements ExpansionSource
{
    private final ExpansionSource parent;
    private final Node source;
    private final RelationshipExpander expander;
    private Iterator<Relationship> relationships;
    private final Relationship howIGotHere;
    private Position position;
    private final int depth;
    private final TraverserImpl traverser;

    ExpansionSourceImpl( TraverserImpl traverser, ExpansionSource parent, Node source,
            RelationshipExpander expander, Relationship toHere )
    {
        this.traverser = traverser;
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
            depth = parent.depth() + 1;
            expandRelationships( true );
        }
    }

    private void expandRelationships( boolean doChecks )
    {
        boolean okToExpand = !doChecks || traverser.shouldExpandBeyond( this );
        relationships = okToExpand ?
                expander.expand( source ).iterator() :
                Collections.<Relationship>emptyList().iterator();
    }

    public ExpansionSource next()
    {
        if ( relationships == null ) // This code will only be executed at the
                                     // start node
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
        while ( relationships.hasNext() )
        {
            Relationship relationship = relationships.next();
            if ( relationship.equals( howIGotHere ) )
            {
                continue;
            }
            Node node = relationship.getOtherNode( source );
            ExpansionSource next = new ExpansionSourceImpl( traverser, this, node,
                    expander, relationship );
            if ( traverser.okToProceed( next ) )
            {
                return next;
            }
        }
        return null;
    }

    public Position position()
    {
        if ( this.position == null )
        {
            this.position = new PositionImpl( this );
        }
        return this.position;
    }

    public int depth()
    {
        return depth;
    }

    public Relationship relationship()
    {
        return howIGotHere;
    }

    public Node node()
    {
        return source;
    }
    
    public ExpansionSource parent()
    {
        return this.parent;
    }
}
