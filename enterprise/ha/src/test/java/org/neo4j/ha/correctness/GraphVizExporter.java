/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ha.correctness;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class GraphVizExporter
{
    private final File target;

    public GraphVizExporter( File target )
    {

        this.target = target;
    }

    public void export(GraphDatabaseService db) throws IOException
    {
        FileOutputStream stream = new FileOutputStream( target );
        PrintWriter out = new PrintWriter( stream );

        out.println("digraph G {");
        out.println("    rankdir=LR;");

        try( Transaction tx = db.beginTx() )
        {
            Set<Node> seen = new HashSet<>();
            Queue<Node> toExplore = new LinkedList<>();
            toExplore.add( db.getNodeById( 0 ) );

            while(toExplore.size() > 0)
            {
                Node current = toExplore.poll();

                out.println( "    " + current.getId() + " [shape=box,label=\"" + current.getProperty( "description" ) + "\"];" );
                for ( Relationship relationship : current.getRelationships() )
                {
                    out.println("    " + current.getId() + " -> " + relationship.getEndNode().getId() + " [label=\""+relationship.getProperty( "description" )+"\"];" );

                    if(!seen.contains( relationship.getEndNode() ))
                    {
                        toExplore.offer( relationship.getEndNode() );
                        seen.add( relationship.getEndNode() );
                    }
                }
            }

            tx.success();
        }

        out.println("}");

        out.flush();
        stream.close();
    }

}
