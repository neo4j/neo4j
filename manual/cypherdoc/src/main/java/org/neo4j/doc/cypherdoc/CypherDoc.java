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
package org.neo4j.doc.cypherdoc;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * Parse AsciiDoc-like content for use in Cypher documentation.
 * 
 * <pre>
 * The string/file is parsed top to bottom.
 * The database isn't flushed: every query builds on the state left
 * behind by the previous ones.
 * 
 * Commands:
 *   // console
 *     Adds an empty div with the class cypherdoc-console to HTML outputs. 
 *   // graph: name
 *     Adds a graphviz graph with "name" in the generated filename.
 *     It will depict whatever state the graph is in at that moment.
 *   Extra lines directly after a query:
 *     The query result will be searched for each of the strings (one string per line).
 * </pre>
 */
public class CypherDoc
{
    static final String EOL = System.getProperty( "line.separator" );

    private CypherDoc()
    {
    }

    /**
     * Parse a string as CypherDoc-enhanced AsciiDoc.
     * 
     * @param input
     * @return
     */
    public static String parse( String input )
    {
        List<Block> blocks = parseBlocks( input );

        StringBuilder output = new StringBuilder( 4096 );
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Transaction transaction = database.beginTx();
        try
        {
            ExecutionEngine engine = new ExecutionEngine( database );

            removeReferenceNode( database );

            for ( Block block : blocks )
            {
                output.append( block.process( engine, database ) )
                        .append( EOL )
                        .append( EOL );
            }

            return output.toString();
        }
        finally
        {
            transaction.finish();
        }
    }

    static List<Block> parseBlocks( String input )
    {
        String[] lines = input.split( EOL );
        if ( lines.length < 3 )
        {
            throw new IllegalArgumentException( "To little content, only "
                                                + lines.length + " lines." );
        }
        List<Block> blocks = new ArrayList<Block>();
        List<String> currentBlock = new ArrayList<String>();
        for ( String line : lines )
        {
            if ( line.trim()
                    .isEmpty() && currentBlock.size() > 0 )
            {
                blocks.add( Block.getBlock( currentBlock ) );
                currentBlock = new ArrayList<String>();
            }
            else
            {
                currentBlock.add( line );
            }
        }
        if ( currentBlock.size() > 0 )
        {
            blocks.add( Block.getBlock( currentBlock ) );
        }
        return blocks;
    }

    @SuppressWarnings( "deprecation" )
    static void removeReferenceNode( GraphDatabaseService database )
    {
        Transaction tx = database.beginTx();
        try
        {
            database.getReferenceNode()
                    .delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
