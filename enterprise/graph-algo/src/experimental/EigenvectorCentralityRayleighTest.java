/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.experimental;

import java.util.Set;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.graphalgo.centrality.EigenvectorCentrality;
import org.neo4j.graphalgo.centrality.EigenvectorCentralityTest;
import org.neo4j.graphalgo.shortestPath.CostEvaluator;

public class EigenvectorCentralityRayleighTest extends
    EigenvectorCentralityTest
{
    public EigenvectorCentralityRayleighTest( String arg0 )
    {
        super( arg0 );
    }

    @Override
    public EigenvectorCentrality getEigenvectorCentrality(
        Direction relationDirection, CostEvaluator<Double> costEvaluator,
        Set<Node> nodeSet, Set<Relationship> relationshipSet, double precision )
    {
        return new EigenvectorCentralityRayleigh( relationDirection,
            costEvaluator, nodeSet, relationshipSet, precision );
    }
}
