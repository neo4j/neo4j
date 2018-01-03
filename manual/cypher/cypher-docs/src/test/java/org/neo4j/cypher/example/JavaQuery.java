/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.example;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;

import static org.neo4j.io.fs.FileUtils.deleteRecursively;

public class JavaQuery
{
    private static final String DB_PATH = "target/java-query-db";
    String resultString;
    String columnsString;
    String nodeResult;
    String rows = "";

    public static void main( String[] args )
    {
        JavaQuery javaQuery = new JavaQuery();
        javaQuery.run();
    }

    void run()
    {
        clearDbPath();

        // START SNIPPET: addData
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );

        try ( Transaction tx = db.beginTx())
        {
            Node myNode = db.createNode();
            myNode.setProperty( "name", "my node" );
            tx.success();
        }
        // END SNIPPET: addData

        // START SNIPPET: execute
        try ( Transaction ignored = db.beginTx();
              Result result = db.execute( "match (n {name: 'my node'}) return n, n.name" ) )
        {
            while ( result.hasNext() )
            {
                Map<String,Object> row = result.next();
                for ( Entry<String,Object> column : row.entrySet() )
                {
                    rows += column.getKey() + ": " + column.getValue() + "; ";
                }
                rows += "\n";
            }
        }
        // END SNIPPET: execute
        // the result is now empty, get a new one
        try ( Transaction ignored = db.beginTx();
              Result result = db.execute( "match (n {name: 'my node'}) return n, n.name" ) )
        {
            // START SNIPPET: items
            Iterator<Node> n_column = result.columnAs( "n" );
            for ( Node node : IteratorUtil.asIterable( n_column ) )
            {
                nodeResult = node + ": " + node.getProperty( "name" );
            }
            // END SNIPPET: items

            // START SNIPPET: columns
            List<String> columns = result.columns();
            // END SNIPPET: columns
            columnsString = columns.toString();
            resultString = db.execute( "match (n {name: 'my node'}) return n, n.name" ).resultAsString();
        }

        db.shutdown();
    }

    private void clearDbPath()
    {
        try
        {
            deleteRecursively( new File( DB_PATH ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
