package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public interface Position
{
    Path path();

    Node node();

    int depth();

    Relationship lastRelationship();

    boolean atStartNode();
}
