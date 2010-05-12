package org.neo4j.graphalgo.path;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

class WeightedPathImpl implements WeightedPath
{
    private final Path path;
    private final double weight;

    WeightedPathImpl( CostEvaluator<Double> costEvaluator, Path path )
    {
        this.path = path;
        double cost = 0;
        for ( Relationship relationship : path.relationships() )
        {
            cost += costEvaluator.getCost( relationship, false );
        }
        this.weight = cost;
    }
    
    WeightedPathImpl( double weight, Path path )
    {
        this.path = path;
        this.weight = weight;
    }

    public double weight()
    {
        return weight;
    }

    public Node getEndNode()
    {
        return path.getEndNode();
    }

    public Node getStartNode()
    {
        return path.getStartNode();
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
}
