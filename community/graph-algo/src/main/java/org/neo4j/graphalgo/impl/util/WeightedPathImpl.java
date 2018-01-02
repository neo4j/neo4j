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
package org.neo4j.graphalgo.impl.util;

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

public class WeightedPathImpl implements WeightedPath
{
    private final Path path;
    private final double weight;

    public WeightedPathImpl( CostEvaluator<Double> costEvaluator, Path path )
    {
        this.path = path;
        double cost = 0;
        for ( Relationship relationship : path.relationships() )
        {
            cost += costEvaluator.getCost( relationship, Direction.OUTGOING );
        }
        this.weight = cost;
    }

    public WeightedPathImpl( double weight, Path path )
    {
        this.path = path;
        this.weight = weight;
    }

    public double weight()
    {
        return weight;
    }

    public Node startNode()
    {
        return path.startNode();
    }

    public Node endNode()
    {
        return path.endNode();
    }

    public Relationship lastRelationship()
    {
        return path.lastRelationship();
    }

    public int length()
    {
        return path.length();
    }

    public Iterable<Node> nodes()
    {
        return path.nodes();
    }
    
    @Override
    public Iterable<Node> reverseNodes()
    {
        return path.reverseNodes();
    }

    public Iterable<Relationship> relationships()
    {
        return path.relationships();
    }
    
    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        return path.reverseRelationships();
    }

    @Override
    public String toString()
    {
        return path.toString() + " weight:" + this.weight;
    }

    public Iterator<PropertyContainer> iterator()
    {
        return path.iterator();
    }
}
