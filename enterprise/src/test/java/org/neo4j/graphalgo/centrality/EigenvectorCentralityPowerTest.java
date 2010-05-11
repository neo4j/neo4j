/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.centrality;

import java.util.Set;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class EigenvectorCentralityPowerTest extends EigenvectorCentralityTest
{
    @Override
    public EigenvectorCentrality getEigenvectorCentrality(
        Direction relationDirection, CostEvaluator<Double> costEvaluator,
        Set<Node> nodeSet, Set<Relationship> relationshipSet, double precision )
    {
        return new EigenvectorCentralityPower( relationDirection,
            costEvaluator, nodeSet, relationshipSet, precision );
    }
}
