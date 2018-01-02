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
package org.neo4j.kernel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;

import static org.neo4j.kernel.Traversal.expanderForTypes;

/**
 * Experimental and very crude utility for building a slightly more powerful
 * expander to use in a traversal.
 * 
 * @author Mattias Persson
 * @deprecated This was an experimental feature which we have rolled back.
 */
@Deprecated
public class PathDescription
{
    private final List<Expander> steps;
    
    public PathDescription()
    {
        this( new ArrayList<Expander>() );
    }
    
    private PathDescription( List<Expander> steps )
    {
        this.steps = steps;
    }
    
    public PathDescription step( RelationshipType type )
    {
        return step( expanderForTypes( type ) );
    }
    
    public PathDescription step( RelationshipType type, Direction direction )
    {
        return step( expanderForTypes( type, direction ) );
    }
    
    public PathDescription step( Expander expander )
    {
        List<Expander> newSteps = new ArrayList<Expander>( steps );
        newSteps.add( expander );
        return new PathDescription( newSteps );
    }
    
    public PathExpander build()
    {
        return new CrudeAggregatedExpander( steps );
    }
    
    private static class CrudeAggregatedExpander implements PathExpander
    {
        private final List<Expander> steps;

        CrudeAggregatedExpander( List<Expander> steps )
        {
            this.steps = steps;
        }
        
        @Override
        public Iterable<Relationship> expand( Path path, BranchState state )
        {
            Expander expansion;
            try
            {
                expansion = steps.get( path.length() );
            }
            catch ( IndexOutOfBoundsException e )
            {
                return Collections.emptyList();
            }
            return expansion.expand( path.endNode() );
        }

        @Override
        public PathExpander reverse()
        {
            List<Expander> reversedSteps = new ArrayList<Expander>();
            for ( Expander step : steps )
                reversedSteps.add( step.reversed() );
            Collections.reverse( reversedSteps );
            return new CrudeAggregatedExpander( reversedSteps );
        }
    }
}
