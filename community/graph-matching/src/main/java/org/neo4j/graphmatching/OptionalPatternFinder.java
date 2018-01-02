/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.graphdb.Node;

@Deprecated
class OptionalPatternFinder
{
    private List<PatternFinder> optionalFinders;
    private List<PatternMatch> currentMatches;
    private Collection<PatternNode> optionalNodes;
    private PatternMatch baseMatch;
    private int position = -1;
    private final PatternMatcher matcher;

    OptionalPatternFinder( PatternMatcher matcher, PatternMatch baseMatch,
        Collection<PatternNode> optionalNodes )
    {
        this.matcher = matcher;
        this.baseMatch = baseMatch;
        this.optionalNodes = optionalNodes;
        initialize();
    }

    private boolean first = true;
    PatternMatch findNextOptionalPatterns()
    {
        if ( position < 0 )
        {
            return null;
        }

        if ( first && anyMatchFound() )
        {
            first = false;
            return PatternMatch.merge( currentMatches );
        }

        boolean found = false;
        for ( ; position >= 0; position-- )
        {
            if ( optionalFinders.get( position ).hasNext() )
            {
                currentMatches.set( position, optionalFinders.get( position )
                    .next() );
                if ( position < currentMatches.size() - 1 )
                {
                    position++;
                    reset( position );
                }
                found = true;
                break;
            }
        }

        if ( !found )
        {
            return null;
        }

        return PatternMatch.merge( currentMatches );
    }

    boolean anyMatchFound()
    {
        return !currentMatches.isEmpty();
    }

    private void initialize()
    {
        optionalFinders = new ArrayList<PatternFinder>();
        currentMatches = new ArrayList<PatternMatch>();

        for ( PatternNode node : optionalNodes )
        {
            PatternFinder finder = new PatternFinder( matcher, node, this
                .getNodeFor( node ), true );
            if ( finder.hasNext() )
            {
                optionalFinders.add( finder );
                currentMatches.add( finder.next() );
                position++;
            }
        }
    }

    private Node getNodeFor( PatternNode node )
    {
        for ( PatternElement element : baseMatch.getElements() )
        {
            if ( node.getLabel().equals( element.getPatternNode().getLabel() ) )
            {
                return element.getNode();
            }
        }
        throw new RuntimeException(
            "Optional graph isn't connected to the main graph." );
    }

    private void reset( int fromIndex )
    {
        for ( int i = fromIndex; i < optionalFinders.size(); i++ )
        {
            PatternFinder finder = optionalFinders.get( i );
            PatternFinder newFinder = new PatternFinder( matcher, finder
                .getStartPatternNode(), finder.getStartNode(), true );
            optionalFinders.set( i, newFinder );
            // Only patterns with matches were added in the first place,
            // so newFinder must have at least one match.
            currentMatches.set( i, newFinder.next() );
            position = i;
        }
    }
}
