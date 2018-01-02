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

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalBranch;

public class ShortestPathsBranchCollisionDetector extends StandardBranchCollisionDetector
{
    private int depth = -1;

    @Deprecated
    public ShortestPathsBranchCollisionDetector( Evaluator evaluator)
    {
        super( evaluator );
    }

    public ShortestPathsBranchCollisionDetector( Evaluator evaluator, Predicate<Path> pathPredicate )
    {
        super( evaluator, pathPredicate );
    }
    
    @Override
    protected boolean includePath( Path path, TraversalBranch startBranch, TraversalBranch endBranch )
    {
        if ( !super.includePath( path, startBranch, endBranch ) )
            return false;
        
        if ( depth == -1 )
        {
            depth = path.length();
            return true;
        }
        return path.length() == depth;
    }
}
