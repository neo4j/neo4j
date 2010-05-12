package org.neo4j.graphalgo.path;

import org.neo4j.graphdb.Path;

public interface WeightedPath extends Path
{
    double weight();
}
