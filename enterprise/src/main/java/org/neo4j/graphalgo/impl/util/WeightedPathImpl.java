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

    public Iterable<Relationship> relationships()
    {
        return path.relationships();
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
