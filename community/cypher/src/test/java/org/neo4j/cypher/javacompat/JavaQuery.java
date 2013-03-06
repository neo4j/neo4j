/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;

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

        // START SNIPPET: execute
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        // add some data first, keep id of node so we can refer to it
        long id;
        Transaction tx = db.beginTx();
        try
        {
            Node refNode = db.createNode();
            id = refNode.getId();
            refNode.setProperty( "name", "reference node" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // let's execute a query now
        ExecutionEngine engine = new ExecutionEngine( db );
        ExecutionResult result = engine.execute( "start n=node("+id+") return n, n.name" );
        // END SNIPPET: execute
        // START SNIPPET: columns
        List<String> columns = result.columns();
        // END SNIPPET: columns
        // START SNIPPET: items
        Iterator<Node> n_column = result.columnAs( "n" );
        for ( Node node : IteratorUtil.asIterable( n_column ) )
        {
            // note: we're grabbing the name property from the node,
            // not from the n.name in this case.
            nodeResult = node + ": " + node.getProperty( "name" );
        }
        // END SNIPPET: items
        // the result is now empty, get a new one
        result = engine.execute( "start n=node("+id+") return n, n.name" );
        // START SNIPPET: rows
        for ( Map<String, Object> row : result )
        {
            for ( Entry<String, Object> column : row.entrySet() )
            {
                rows += column.getKey() + ": " + column.getValue() + "; ";
            }
            rows += "\n";
        }
        // END SNIPPET: rows
        resultString = engine.execute( "start n=node("+id+") return n, n.name" ).dumpToString();
        columnsString = columns.toString();
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
