package org.neo4j.procedure.impl;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Resource;

public class ProcedureExample
{
    @Resource
    public GraphDatabaseService db;

    /**
     * Finds all nodes in the database with more relationships than the specified threshold.
     * @param threshold only include nodes with at least this many relationships
     * @return a stream of records describing supernodes in this database
     */
    @Procedure
    public Stream<SuperNode> findSuperNodes( @Name("threshold") long threshold )
    {
        return db.getAllNodes().stream()
                .filter( (node) -> node.getDegree() > threshold )
                .map( SuperNode::new );
    }

    /**
     * Output record for {@link #findSuperNodes(long)}.
     */
    public static class SuperNode
    {
        public long nodeId;
        public long degree;

        public SuperNode( Node node )
        {
            this.nodeId = node.getId();
            this.degree = node.getDegree();
        }
    }
}
