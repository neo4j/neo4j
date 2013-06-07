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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.visualization.asciidoc.AsciidocHelper;
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle;
import org.neo4j.visualization.graphviz.GraphvizWriter;
import org.neo4j.walk.Walker;

enum BlockType
{
    TITLE
    {
        @Override
        boolean isA( List<String> block )
        {
            int size = block.size();
            return size > 0 && ( block.get( 0 )
                    .startsWith( "=" ) || ( size > 1 && block.get( 1 )
                    .startsWith( "=" ) ) );
        }

        @Override
        String process( Block block, ExecutionEngine engine,
                GraphDatabaseService database )
        {
            String title = block.lines.get( 0 )
                    .replace( "=", "" )
                    .trim();
            String id = "cypherdoc-" + title.toLowerCase()
                    .replace( ' ', '-' );
            return "[[" + id + "]]" + CypherDoc.EOL + "= " + title + " ="
                   + CypherDoc.EOL;
        }
    },
    QUERY
    {
        @Override
        boolean isA( List<String> block )
        {
            String first = block.get( 0 );
            if ( !first.startsWith( "[" ) )
            {
                return false;
            }
            if ( first.contains( "source" ) && first.contains( "cypher" ) )
            {
                return true;
            }
            if ( block.size() > 4 && first.startsWith( "[[" ) )
            {
                String second = block.get( 1 );
                if ( second.contains( "source" ) && second.contains( "cypher" ) )
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        String process( Block block, ExecutionEngine engine,
                GraphDatabaseService database )
        {
            List<String> queryHeader = new ArrayList<String>();
            List<String> queryLines = new ArrayList<String>();
            List<String> testLines = new ArrayList<String>();
            boolean includeResult = false;
            boolean queryStarted = false;
            boolean queryEnded = false;
            for ( String line : block.lines )
            {
                if ( !queryStarted )
                {
                    if ( line.startsWith( "[" ) && line.contains( "source" )
                         && line.contains( "cypher" )
                         && line.contains( "includeresult" ) )
                    {
                        includeResult = true;
                        // "includeresult" isn't valid AsciiDoc, let's get rid
                        // of it
                        line = "[source,cypher]";
                    }
                    if ( line.startsWith( "----" ) )
                    {
                        queryStarted = true;
                    }
                    else
                    {
                        queryHeader.add( line );
                    }
                }
                else if ( queryStarted && !queryEnded )
                {
                    if ( line.startsWith( "----" ) )
                    {
                        queryEnded = true;
                    }
                    else
                    {
                        queryLines.add( line );
                    }
                }
                else if ( queryEnded )
                {
                    testLines.add( line );
                }
            }
            String query = StringUtils.join( queryLines, CypherDoc.EOL );
            String result = engine.execute( query )
                    .dumpToString();
            for ( String test : testLines )
            {
                if ( !result.contains( test ) )
                {
                    throw new IllegalArgumentException(
                            "Query result doesn't contain the string: '" + test
                                    + "'. The query:" + block.toString()
                                    + CypherDoc.EOL + CypherDoc.EOL + result );
                }

            }
            String prettifiedQuery = engine.prettify( query );
            StringBuilder output = new StringBuilder( 512 );
            output.append( StringUtils.join( queryHeader, CypherDoc.EOL ) )
                    .append( CypherDoc.EOL )
                    .append( "----" )
                    .append( CypherDoc.EOL )
                    .append( prettifiedQuery )
                    .append( CypherDoc.EOL )
                    .append( "----" )
                    .append( CypherDoc.EOL )
                    .append( CypherDoc.EOL );

            if ( includeResult )
            {
                output.append( AsciidocHelper.createQueryResultSnippet( result ) );
            }
            return output.toString();
        }
    },
    GRAPH
    {
        @Override
        boolean isA( List<String> block )
        {
            String first = block.get( 0 );
            return first.startsWith( "//" ) && first.contains( "graph:" );
        }

        @Override
        String process( Block block, ExecutionEngine engine,
                GraphDatabaseService database )
        {
            String first = block.lines.get( 0 );
            String id = first.substring( first.indexOf( "graph:" ) + 6 )
                    .trim();
            GraphvizWriter writer = new GraphvizWriter(
                    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors() );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try
            {
                writer.emit( out, Walker.fullGraph( database ) );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
            StringBuilder output = new StringBuilder( 512 );
            try
            {
                String dot = out.toString( "UTF-8" );
                output.append( "[\"dot\", \"cypherdoc-" )
                        .append( id )
                        .append( '-' )
                        .append( Integer.toHexString( dot.hashCode() ) )
                        .append( ".svg\", \"neoviz\"]\n----\n" )
                        .append( dot )
                        .append( "----\n" );
            }
            catch ( UnsupportedEncodingException e )
            {
                e.printStackTrace();
            }
            return output.toString();
        }
    },
    CONSOLE
    {
        @Override
        boolean isA( List<String> block )
        {
            String first = block.get( 0 );
            return first.startsWith( "//" ) && first.contains( "console" );
        }

        @Override
        String process( Block block, ExecutionEngine engine,
                GraphDatabaseService database )
        {
            return StringUtils.join(
                    new String[] {
                            "ifdef::backend-html,backend-html5,backend-xhtml11,backend-deckjs[]",
                            "++++",
                            "<p class=\"cypherdoc-console\"></p>",
                            "++++", "endif::[]",
                            "ifndef::backend-html,backend-html5,backend-xhtml11,backend-deckjs[]",
                            "++++",
                            "<simpara role=\"cypherdoc-console\"></simpara>",
                            "++++", "endif::[]" },
                    CypherDoc.EOL )
                   + CypherDoc.EOL;
        }

    },
    TEXT
    {
        @Override
        boolean isA( List<String> block )
        {
            return true;
        }
    };

    abstract boolean isA( List<String> block );

    String process( Block block, ExecutionEngine engine,
            GraphDatabaseService database )
    {
        return StringUtils.join( block.lines, CypherDoc.EOL ) + CypherDoc.EOL;
    }
}
