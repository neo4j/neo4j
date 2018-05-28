/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_PRUNE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;

public class TestProcedure
{
    @Context
    public GraphDatabaseService db;

    @Procedure( "org.neo4j.movieTraversal" )
    @Description( "org.neo4j.movieTraversal" )
    public Stream<PathResult> movieTraversal( @Name( "start" ) Node start ) throws Exception
    {
        TraversalDescription td =
                db.traversalDescription()
                        .breadthFirst()
                        .relationships( RelationshipType.withName( "ACTED_IN" ), Direction.BOTH )
                        .relationships( RelationshipType.withName( "PRODUCED" ), Direction.BOTH )
                        .relationships( RelationshipType.withName( "DIRECTED" ), Direction.BOTH )
                        .evaluator( Evaluators.fromDepth( 3 ) )
                        .evaluator( new LabelEvaluator( "Western", 1, 3 ) )
                        .uniqueness( Uniqueness.NODE_GLOBAL );

        return td.traverse( start ).stream().map( PathResult::new );
    }

    public static class PathResult
    {
        public Path path;

        PathResult( Path path )
        {
            this.path = path;
        }
    }

    public static class LabelEvaluator implements Evaluator
    {
        private Set<String> endNodeLabels;
        private long limit = -1;
        private long minLevel = -1;
        private long resultCount = 0;

        LabelEvaluator( String endNodeLabel, long limit, int minLevel )
        {
            this.limit = limit;
            this.minLevel = minLevel;

            endNodeLabels = Collections.singleton( endNodeLabel );
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            int depth = path.length();
            Node check = path.endNode();

            if ( depth < minLevel )
            {
                return EXCLUDE_AND_CONTINUE;
            }

            if ( limit != -1 && resultCount >= limit )
            {
                return EXCLUDE_AND_PRUNE;
            }

            return labelExists( check, endNodeLabels ) ? countIncludeAndContinue() : EXCLUDE_AND_CONTINUE;
        }

        private boolean labelExists( Node node, Set<String> labels )
        {
            if ( labels.isEmpty() )
            {
                return false;
            }

            for ( Label lab : node.getLabels() )
            {
                if ( labels.contains( lab.name() ) )
                {
                    return true;
                }
            }
            return false;
        }

        private Evaluation countIncludeAndContinue()
        {
            resultCount++;
            return INCLUDE_AND_CONTINUE;
        }
    }
}
