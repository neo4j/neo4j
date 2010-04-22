package org.neo4j.graphdb;

public interface Path
{
    Node getStartNode();

    Node getEndNode();

    Iterable<Relationship> relationships();

    Iterable<Node> nodes();

    /**
     * @return the number of relationships in the path.
     */
    int length();

    /**
     * Return a natural string representation of this Path.
     * 
     * The string representation should show all the nodes and relationships in
     * the path with relationship directions represented.
     * 
     * @return A string representation of the path.
     */
    @Override
    public String toString();
}
