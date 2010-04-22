package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public interface Traverser extends Iterable<Position>
{
    Iterable<Node> nodes();
    
    Iterable<Relationship> relationships();
    
    Iterable<Path> paths();
}
