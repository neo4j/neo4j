package org.neo4j.graphalgo;

import org.neo4j.graphdb.Path;

public interface WeightedPath extends Path
{
    double weight();
}
