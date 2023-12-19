/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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

    public void export( GraphDatabaseService db ) throws IOException
    {
        FileOutputStream stream = new FileOutputStream( target );
        PrintWriter out = new PrintWriter( stream );

        out.println("digraph G {");
        out.println("    rankdir=LR;");

        try ( Transaction tx = db.beginTx() )
        {
            Set<Node> seen = new HashSet<>();
            Queue<Node> toExplore = new LinkedList<>();
            toExplore.add( db.getNodeById( 0 ) );

            while ( toExplore.size() > 0 )
            {
                Node current = toExplore.poll();

                out.println( "    " + current.getId() + " [shape=box,label=\"" +
                        current.getProperty( "description" ) + "\"];" );
                for ( Relationship relationship : current.getRelationships() )
                {
                    out.println( "    " + current.getId() + " -> " + relationship.getEndNode().getId() + " [label=\"" +
                            relationship.getProperty( "description" ) + "\"];" );

                    if ( !seen.contains( relationship.getEndNode() ) )
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
