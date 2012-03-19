/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.graphmatching;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * Performs the actual finding of matches given the pattern of how a match
 * looks like and a {@link Node} to start traversing from.
 */
@Deprecated
class PatternFinder implements Iterable<PatternMatch>, Iterator<PatternMatch>
{
    private Set<Relationship> visitedRels = new HashSet<Relationship>();
    private PatternPosition currentPosition;
    private OptionalPatternFinder optionalFinder;
    private PatternNode startPatternNode;
    private Node startNode;
    private Collection<PatternNode> optionalNodes;
    private boolean optional;
    private final PatternMatcher matcher;

    PatternFinder( PatternMatcher matcher, PatternNode start, Node startNode )
    {
        this( matcher, start, startNode, false );
    }

    PatternFinder( PatternMatcher matcher, PatternNode start, Node startNode,
        boolean optional )
    {
        this.matcher = matcher;
        this.startPatternNode = start;
        this.startNode = startNode;
        currentPosition = new PatternPosition( startNode, start, optional );
        this.optional = optional;
    }

    PatternFinder( PatternMatcher matcher, PatternNode start, Node startNode,
        boolean optional, Collection<PatternNode> optionalNodes )
    {
        this( matcher, start, startNode, optional );
        this.optionalNodes = optionalNodes;
    }

    PatternNode getStartPatternNode()
    {
        return startPatternNode;
    }

    Node getStartNode()
    {
        return startNode;
    }

    /**
     * Represents a traversal state so that we can go back to it when we've
     * descended the graph and comes back up to continue matching on a
     * higher level.
     */
    private static class CallPosition
    {
        private PatternPosition patternPosition;
        private Iterator<Relationship> relItr;
        private Relationship lastRel;
        private PatternRelationship currentPRel;
        private boolean popUncompleted;

        CallPosition( PatternPosition patternPosition, Relationship lastRel,
            Iterator<Relationship> relItr, PatternRelationship currentPRel,
            boolean popUncompleted )
        {
            this.patternPosition = patternPosition;
            this.relItr = relItr;
            this.lastRel = lastRel;
            this.currentPRel = currentPRel;
            this.popUncompleted = popUncompleted;
        }

        public void setLastVisitedRelationship( Relationship rel )
        {
            this.lastRel = rel;
        }

        public Relationship getLastVisitedRelationship()
        {
            return lastRel;
        }

        public boolean shouldPopUncompleted()
        {
            return popUncompleted;
        }

        public PatternPosition getPatternPosition()
        {
            return patternPosition;
        }

        public PatternRelationship getPatternRelationship()
        {
            return currentPRel;
        }

        public Iterator<Relationship> getRelationshipIterator()
        {
            return relItr;
        }
    }

    private Stack<CallPosition> callStack = new Stack<CallPosition>();
    private Stack<PatternPosition> uncompletedPositions =
        new Stack<PatternPosition>();
    private Stack<PatternElement> foundElements = new Stack<PatternElement>();

    private PatternMatch findNextMatch()
    {
        if ( callStack.isEmpty() && currentPosition != null )
        {
            // Try to find a first indication of a match, i.e. find some part
            // of the pattern in the graph.
            if ( traverse( currentPosition, true ) )
            {
                // found first match, return it
                currentPosition = null;
                return extractPotentialResult();
            }
            currentPosition = null;
        }
        else
        {
            return traverseFromCallStack();
        }
        return null;
    }

    private PatternMatch extractPotentialResult()
    {
        HashMap<PatternNode, PatternElement> filteredElements =
            new HashMap<PatternNode, PatternElement>();
        HashMap<PatternRelationship, Relationship> relElements =
            new HashMap<PatternRelationship, Relationship>();
        boolean patternValid = true;
        for ( PatternElement element : foundElements )
        {
            PatternElement other = filteredElements.get( element.getPatternNode() );
            if ( other != null && !other.getNode().equals( element.getNode() ) )
            {
                patternValid = false;
                break;
            }
            filteredElements.put( element.getPatternNode(), element );
            relElements.put( element.getFromPatternRelationship(),
                element.getFromRelationship() );
        }
        PatternMatch patternMatch = new PatternMatch( filteredElements,
            relElements );
        foundElements.pop();
        if ( patternValid )
        {
            return patternMatch;
        }
        return traverseFromCallStack();
    }

    private PatternMatch traverseFromCallStack()
    {
        if ( callStack.isEmpty() )
        {
            return null;
        }
        boolean matchFound = false;
        do
        {
            CallPosition callStackInformation = callStack.peek();
            matchFound = traverse( callStackInformation );
        }
        while ( !callStack.isEmpty() && !matchFound );
        if ( matchFound )
        {
            return extractPotentialResult();
        }
        return null;
    }

    private boolean traverse( CallPosition callPos )
    {
        // make everything like it was before we returned previous match
        PatternPosition currentPos = callPos.getPatternPosition();
        PatternRelationship pRel = callPos.getPatternRelationship();
        pRel.mark();
        visitedRels.remove( callPos.getLastVisitedRelationship() );
        Node currentNode = currentPos.getCurrentNode();
        Iterator<Relationship> relItr = callPos.getRelationshipIterator();
        while ( relItr.hasNext() )
        {
            Relationship rel = relItr.next();
            if ( visitedRels.contains( rel ) )
            {
                continue;
            }
            if ( !checkProperties( pRel, rel ) )
            {
                continue;
            }
            Node otherNode = rel.getOtherNode( currentNode );
            PatternNode otherPosition = pRel.getOtherNode( currentPos
                .getPatternNode() );
            pRel.mark();
            visitedRels.add( rel );
            if ( traverse( new PatternPosition( otherNode, otherPosition, pRel,
                rel, optional ), true ) )
            {
                callPos.setLastVisitedRelationship( rel );
                return true;
            }
            visitedRels.remove( rel );
            pRel.unMark();
        }
        pRel.unMark();
        if ( callPos.shouldPopUncompleted() )
        {
            uncompletedPositions.pop();
        }
        callStack.pop();
        foundElements.pop();
        return false;
    }

    private boolean traverse( PatternPosition currentPos, boolean pushElement )
    {
        PatternNode pNode = currentPos.getPatternNode();
        Node currentNode = currentPos.getCurrentNode();

        if ( !checkProperties( pNode, currentNode ) )
        {
            return false;
        }
        if ( pushElement )
        {
            foundElements.push( new PatternElement(
                pNode, currentPos.fromPatternRel(),
                currentNode, currentPos.fromRelationship() ) );
        }
        if ( currentPos.hasNext() )
        {
            boolean popUncompleted = false;
            PatternRelationship pRel = currentPos.next();
            if ( currentPos.hasNext() )
            {
                uncompletedPositions.push( currentPos );
                popUncompleted = true;
            }
            assert !pRel.isMarked();
            Iterator<Relationship> relItr = getRelationshipIterator( currentPos
                .getPatternNode(), currentNode, pRel );
            pRel.mark();
            while ( relItr.hasNext() )
            {
                Relationship rel = relItr.next();
                if ( visitedRels.contains( rel ) )
                {
                    continue;
                }
                if ( !checkProperties( pRel, rel ) )
                {
                    continue;
                }
                Node otherNode = rel.getOtherNode( currentNode );
                PatternNode otherPosition = pRel.getOtherNode( currentPos
                    .getPatternNode() );
                visitedRels.add( rel );

                CallPosition callPos = new CallPosition( currentPos, rel,
                    relItr, pRel, popUncompleted );
                callStack.push( callPos );
                if ( traverse( new PatternPosition( otherNode, otherPosition,
                    pRel, rel, optional ), true ) )
                {
                    return true;
                }
                callStack.pop();
                visitedRels.remove( rel );
            }
            pRel.unMark();
            if ( popUncompleted )
            {
                uncompletedPositions.pop();
            }
            foundElements.pop();
            return false;
        }
        boolean matchFound = true;
        if ( !uncompletedPositions.isEmpty() )
        {
            PatternPosition digPos = uncompletedPositions.pop();
            digPos.reset();
            matchFound = traverse( digPos, false );
            uncompletedPositions.push( digPos );
            return matchFound;
        }
        return true;
    }

    private Iterator<Relationship> getRelationshipIterator(
        PatternNode fromNode, Node currentNode, PatternRelationship pRel )
    {
        Iterator<Relationship> relItr = null;
        if ( pRel.anyRelType() )
        {
            relItr = currentNode.getRelationships(
                    pRel.getDirectionFrom( fromNode ) ).iterator();
        }
        else
        {
            relItr = currentNode.getRelationships( pRel.getType(),
                    pRel.getDirectionFrom( fromNode ) ).iterator();
        }
        return relItr;
    }

    private boolean checkProperties(
            AbstractPatternObject<? extends PropertyContainer> patternObject,
            PropertyContainer object )
    {
        PropertyContainer associatedObject = patternObject.getAssociation();
        if ( associatedObject != null && !object.equals( associatedObject ) )
        {
            return false;
        }

        for ( Map.Entry<String, Collection<ValueMatcher>> matchers :
                patternObject.getPropertyConstraints() )
        {
            String key = matchers.getKey();
            Object propertyValue = object.getProperty( key, null );
            for (ValueMatcher matcher : matchers.getValue() )
            {
                if ( !matcher.matches( propertyValue ) )
                {
                    return false;
                }
            }
        }

        return true;
    }

    public Iterator<PatternMatch> iterator()
    {
        return this;
    }

    private PatternMatch match = null;
    private PatternMatch optionalMatch = null;

    public boolean hasNext()
    {
        if ( match == null )
        {
            match = findNextMatch();
            optionalFinder = null;
        }
        else if ( optionalNodes != null )
        {
            if ( optionalFinder == null )
            {
                optionalFinder = new OptionalPatternFinder( matcher, match,
                    optionalNodes );
            }
            if ( optionalMatch == null )
            {
                optionalMatch = optionalFinder.findNextOptionalPatterns();
            }
            if ( optionalMatch == null && optionalFinder.anyMatchFound() )
            {
                match = null;
                return hasNext();
            }
        }
        return match != null;
    }

    public PatternMatch next()
    {
        if ( match == null )
        {
            match = findNextMatch();
            optionalFinder = null;
        }

        PatternMatch matchToReturn = match;
        PatternMatch optionalMatchToReturn = null;
        if ( match != null && optionalNodes != null )
        {
            if ( optionalFinder == null )
            {
                optionalFinder = new OptionalPatternFinder( matcher, match,
                    optionalNodes );
            }
            if ( optionalMatch == null )
            {
                optionalMatch = optionalFinder.findNextOptionalPatterns();
            }
            optionalMatchToReturn = optionalMatch;
            optionalMatch = null;
            if ( optionalMatchToReturn == null )
            {
                match = null;
                if ( optionalFinder.anyMatchFound() )
                {
                    return next();
                }
            }
        }
        else
        {
            match = null;
        }
        if ( matchToReturn == null )
        {
            throw new NoSuchElementException();
        }
        return optionalMatchToReturn != null ? PatternMatch.merge(
            matchToReturn, optionalMatchToReturn ) : matchToReturn;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
