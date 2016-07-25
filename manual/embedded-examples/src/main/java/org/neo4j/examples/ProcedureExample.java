/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Context;

public class ProcedureExample
{
    // START SNIPPET: procedureExample
    @Context
    public GraphDatabaseService db;

    /**
     * Finds all nodes in the database with more relationships than the specified threshold.
     * @param threshold only include nodes with at least this many relationships
     * @return a stream of records describing dense nodes in this database
     */
    @Procedure
    public Stream<DenseNode> findDenseNodes( @Name("threshold") long threshold )
    {
        return db.getAllNodes().stream()
                .filter( (node) -> node.getDegree() > threshold )
                .map( DenseNode::new );
    }
    // END SNIPPET: procedureExample

    // START SNIPPET: outputRecordExample
    /**
     * Output record for {@link #findDenseNodes(long)}.
     */
    public static class DenseNode
    {
        public long nodeId;
        public long degree;

        public DenseNode( Node node )
        {
            this.nodeId = node.getId();
            this.degree = node.getDegree();
        }
    }
    // END SNIPPET: outputRecordExample
}
