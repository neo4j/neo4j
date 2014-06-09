/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Transaction;
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
            return size > 0 && ( ( block.get( 0 )
                    .startsWith( "=" ) && !block.get( 0 )
                    .startsWith( "==" ) ) || size > 1 && block.get( 1 )
                    .startsWith( "=" ) );
        }

        @Override
        String process( Block block, State state )
        {
            String title = block.lines.get( 0 )
                    .replace( "=", "" )
                    .trim();
            String id = "cypherdoc-" + title.toLowerCase().replace( ' ', '-' );
            return "[[" + id + "]]" + CypherDoc.EOL + "= " + title + " ="
                   + CypherDoc.EOL;
        }
    },
    HIDE
    {
        @Override
        String process( Block block, State state )
        {
            return OutputHelper.passthroughMarker( "hide-query", "span", "simpara" );
        }

        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "hide" );
        }
    },
    SETUP
    {
        @Override
        String process( Block block, State state )
        {
            return OutputHelper.passthroughMarker( "setup-query", "span", "simpara" );
        }

        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "setup" );
        }
    },
    OUTPUT
    {
        @Override
        String process( Block block, State state )
        {
            return OutputHelper.passthroughMarker( "query-output", "span", "simpara" );
        }

        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "output" );
        }
    },
    PROFILE
            {
                @Override
                String process( Block block, State state )
                {
                    return AsciidocHelper.createOutputSnippet( state.latestResult.profile );
                }

                @Override
                boolean isA( List<String> block )
                {
                    return isACommentWith( block, "profile" );
                }
            },
    TABLE
    {
        @Override
        String process( Block block, State state )
        {
            return AsciidocHelper.createQueryResultSnippet( state.latestResult.text );
        }

        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "table" );
        }
    },
    TEST
    {
        @Override
        String process( Block block, State state )
        {
            List<String> tests = block.lines.subList( 1, block.lines.size() - 1 );
            String result = state.latestResult.text;
            List<String> failures = new ArrayList<>();
            for ( String test : tests )
            {
                if ( !result.contains( test ) )
                {
                    failures.add( test );
                }
            }
            if ( !failures.isEmpty() )
            {
                throw new TestFailureException( state.latestResult, failures );
            }
            return "";
        }

        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "//" );
        }
    },
    QUERY
    {
        @Override
        boolean isA( List<String> block )
        {
            String first = block.get( 0 );
            if ( first.charAt( 0 ) != '[' )
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
        String process( Block block, State state )
        {
            List<String> queryLines = new ArrayList<>();
            boolean queryStarted = false;
            for ( String line : block.lines )
            {
                if ( !queryStarted )
                {
                    if ( line.startsWith( CODE_BLOCK ) )
                    {
                        queryStarted = true;
                    }
                }
                else
                {
                    if ( line.startsWith( CODE_BLOCK ) )
                    {
                        break;
                    }
                    else
                    {
                        queryLines.add( line );
                    }
                }
            }
            String query = StringUtils.join( queryLines, CypherDoc.EOL );
            String webQuery = query;
            String fileQuery = query;
            for ( String file : state.knownFiles )
            {
                File absolutePath = new File( state.parentDirectory, file );
                String fileUrl = absolutePath.toURI().toString();
                fileQuery = fileQuery.replace( file, fileUrl );
                webQuery = webQuery.replace( file, state.url + file );
            }

            state.latestResult = new Result( fileQuery, state.engine.profile( fileQuery ) );
            String prettifiedQuery = state.engine.prettify( webQuery );

            try ( Transaction tx = state.database.beginTx() )
            {
                state.database.schema().awaitIndexesOnline( 2000, TimeUnit.MILLISECONDS );
                tx.success();
            }


            return AsciidocHelper.createCypherSnippetFromPreformattedQuery( prettifiedQuery ) + CypherDoc.EOL +
                    CypherDoc.EOL;
        }
    },
    GRAPH
    {
        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "graph" );
        }

        @Override
        String process( Block block, State state )
        {
            String first = block.lines.get( 0 );
            String id = "";
            if ( first.length() > 8 )
            {
                id = first.substring( first.indexOf( "graph" ) + 5 ).trim();
                if ( id.indexOf( ':' ) != -1 )
                {
                    id = first.substring( first.indexOf( ':' ) + 1 ).trim();
                }
            }
            GraphvizWriter writer = new GraphvizWriter(
                    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors() );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (Transaction tx = state.database.beginTx())
            {
                writer.emit( out, Walker.fullGraph( state.database ) );
                tx.success();
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
            return isACommentWith( block, "console" );
        }

        @Override
        String process( Block block, State state )
        {
            return OutputHelper.passthroughMarker( "cypherdoc-console", "p", "simpara" );
        }
    },
    FILE
            {
                @Override
                boolean isA( List<String> block )
                {
                    return isACommentWith( block, "file:" );
                }

                @Override
                String process( Block block, State state )
                {
                    String fileLine = block.lines.get( 0 );
                    String[] split = fileLine.split( ":" );
                    assert split.length == 2;
                    String fileName = split[1];
                    state.knownFiles.add( fileName );
                    return "";
                }
            },
    TEXT
    {
        @Override
        boolean isA( List<String> block )
        {
            return true;
        }

        @Override
        String process( Block block, State state )
        {
            return StringUtils.join( block.lines, CypherDoc.EOL ) + CypherDoc.EOL;
        }
    };

    private static final String CODE_BLOCK = "----";

    abstract boolean isA( List<String> block );

    abstract String process( Block block, State state );

    private static boolean isACommentWith( List<String> block, String command )
    {
        String first = block.get( 0 );
        return first.startsWith( "//" + command ) || first.startsWith( "// " + command );
    }
}
