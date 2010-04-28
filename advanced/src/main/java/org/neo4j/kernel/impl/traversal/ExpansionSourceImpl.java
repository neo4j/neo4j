package org.neo4j.kernel.impl.traversal;

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.TraversalRules;

class ExpansionSourceImpl implements ExpansionSource
{
    private final ExpansionSource parent;
    private final Node source;
    private final RelationshipExpander expander;
    private Iterator<Relationship> relationships;
    private final Relationship howIGotHere;
    private Position position;
    private final int depth;

    ExpansionSourceImpl( TraversalRules rules, ExpansionSource parent, Node source,
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
            depth = parent.depth() + 1;
            expandRelationships( rules, true );
        }
    }

    private void expandRelationships( TraversalRules rules, boolean doChecks )
    {
        boolean okToExpand = !doChecks || rules.shouldExpandBeyond( this );
        relationships = okToExpand ?
                expander.expand( source ).iterator() :
                Collections.<Relationship>emptyList().iterator();
    }

    public ExpansionSource next( TraversalRules rules )
    {
        if ( relationships == null ) // This code will only be executed at the
                                     // start node
        {
            if ( ((TraversalRulesImpl) rules).uniquness.type == PrimitiveTypeFetcher.RELATIONSHIP
                 || rules.okToProceed( this ) )
            {
                expandRelationships( rules, false );
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
            ExpansionSource next = new ExpansionSourceImpl( rules, this, node,
                    expander, relationship );
            if ( rules.okToProceed( next ) )
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
