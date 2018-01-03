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
package org.neo4j.doc.cypherdoc;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

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
            return size > 0 && 
                ( ( block.get( 0 ).startsWith( "=" ) 
                 && !block.get( 0 ).startsWith( "==" )));
        }

        @Override
        String process( Block block, State state )
        {
            String title = block.lines.get( 0 )
                    .replace( "=", "" )
                    .trim();
            String id = "cypherdoc-" + title.replace( ' ', '-' ).replaceAll( "[^\\w-]", "" ).toLowerCase();
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
    SQL_TABLE
    {
        @Override
        String process( Block block, State state )
        {
            return AsciidocHelper.createQueryResultSnippet( state.latestSqlResult.text );
        }

        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "sqltable" );
        }
    },
    QUERYTEST
    {
        @Override
        String process( Block block, State state )
        {
            if ( state.latestResult == null && state.latestSqlResult == null )
            {
                throw new IllegalArgumentException( "Nothing to test" );
            }
            List<String> tests = block.lines.subList( 1, block.lines.size() - 1 );
            boolean checkCypherResults = state.latestResult != null && state.latestResult != state.testedResult;
            boolean checkSqlResults = state.latestSqlResult != null && state.latestSqlResult != state.testedSqlResult;
            String result = checkCypherResults ? state.latestResult.text : null;
            String sqlResult = checkSqlResults ? state.latestSqlResult.text : null;
            List<String> failures = new ArrayList<>();
            List<String> sqlFailures = new ArrayList<>();
            for ( String test : tests )
            {
                if ( result != null && !result.contains( test ) )
                {
                    failures.add( test );
                }
                if ( sqlResult != null && !sqlResult.contains( test ) )
                {
                    sqlFailures.add( test );
                }
            }
            if ( !failures.isEmpty() )
            {
                throw new TestFailureException( state.latestResult, failures );
            }
            if ( !sqlFailures.isEmpty() )
            {
                throw new TestFailureException( state.latestSqlResult, sqlFailures );
            }

            state.testedResult = state.latestResult;
            state.testedSqlResult = state.latestSqlResult;

            return "";
        }

        @Override
        boolean isA( List<String> block )
        {
            return isCodeBlock( "querytest", block );
        }
    },
    PROFILETEST
    {
        @Override
        String process( Block block, State state )
        {
            if ( state.latestResult == null )
            {
                throw new IllegalArgumentException( "Nothing to test" );
            }
            List<String> tests = block.lines.subList( 1, block.lines.size() - 1 );
            String profile = state.latestResult.profile;
            List<String> failures = new ArrayList<>();
            for ( String test : tests )
            {
                if ( profile != null && !profile.contains( test ) )
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
            return isCodeBlock( "profiletest", block );
        }
    },
    PARAMETERS
    {
        @Override
        boolean isA( List<String> block )
        {
            String first = block.get( 0 );
            return isCodeBlock( "json", block ) && first.contains( "role" )
                   && first.contains( "parameters" );
        }

        @Override
        String process( Block block, State state )
        {
            String json = getRawCodeBlockContent( block );
            try
            {
                state.parameters.clear();
                state.parameters.putAll( JSON_MAPPER.readValue( json, HashMap.class ) );
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            String prettifiedJson = null;
            try
            {
                prettifiedJson = JSON_WRITER.writeValueAsString( state.parameters );
            }
            catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return "\n[source,json]\n----\n" + ( prettifiedJson == null ? json : prettifiedJson ) + "\n----\n\n";
        }
    },
    CYPHER
    {
        @Override
        boolean isA( List<String> block )
        {
            return isCodeBlock( "cypher", block );
        }

        @Override
        String process( Block block, State state )
        {
            String firstLine = block.lines.get( 0 );
            boolean exec = !(firstLine.contains( "noexec" ) || firstLine.contains( "hideexec" ));
            List<String> statements = getQueriesBlockContent( block );
            List<String> prettifiedStatements = new ArrayList<>();
            String webQuery = null;
            String fileQuery = null;
            for ( String query : statements )
            {
                webQuery = query;
                fileQuery = query;
                for ( String file : state.knownFiles )
                {
                    File absolutePath = new File( state.parentDirectory, file );
                    String fileUrl = absolutePath.toURI().toString();
                    fileQuery = replaceFilename( fileQuery, file, fileUrl );
                    webQuery = replaceFilename( webQuery, file, state.url + file );
                }
                if ( exec )
                {
                    state.latestResult =
                            new Result( fileQuery, state.engine.profile( fileQuery, state.parameters ), state.database );
                    prettifiedStatements.add( state.engine.prettify( webQuery ) );
                    try (Transaction tx = state.database.beginTx())
                    {
                        state.database.schema().awaitIndexesOnline( 10000, TimeUnit.SECONDS );
                        tx.success();
                    }
                }
                else
                {
                    prettifiedStatements.add( webQuery );
                }
            }


            state.parameters.clear();
            String cypher = StringUtils.join( prettifiedStatements, CypherDoc.EOL );
            return AsciidocHelper.createCypherSnippetFromPreformattedQuery( cypher, exec ) + CypherDoc.EOL
                   + CypherDoc.EOL;
        }
    },
    SQL
    {
        @Override
        boolean isA( List<String> block )
        {
            return isCodeBlock( "sql", block );
        }


        @Override
        String process( Block block, State state )
        {
            List<String> statements = getQueriesBlockContent( block );
            for ( String query : statements )
            {
                String result = executeSql( query, state.sqlDatabase );
                state.latestSqlResult = new Result( query, result );
            }
            String printQuery = StringUtils.join( statements, CypherDoc.EOL );
            return AsciidocHelper.createSqlSnippet( printQuery ) + CypherDoc.EOL + CypherDoc.EOL;
        }
    },
    GRAPH_RESULT
    {
        @Override
        boolean isA( List<String> block )
        {
            return isACommentWith( block, "graph_result" );
        }

        @Override
        String process( Block block, State state )
        {
            return writeGraph( block, state, true );
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
            return writeGraph( block, state, false );
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
    private static final int COLUMN_MAX_WIDTH = 25;
    private static final String LINE_SEGMENT = new String( new char[COLUMN_MAX_WIDTH] ).replace( '\0', '-' );
    private static final String SPACE_SEGMENT = new String( new char[COLUMN_MAX_WIDTH] ).replace( '\0', ' ' );
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ObjectWriter JSON_WRITER = JSON_MAPPER.writerWithDefaultPrettyPrinter();

    abstract boolean isA( List<String> block );

    abstract String process( Block block, State state );

    private static String writeGraph( Block block, State state, boolean resultOnly )
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
            else if ( id.startsWith( "_result" ) )
            {
                id = "result";
            }
        }
        GraphvizWriter writer = new GraphvizWriter(
                AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors() );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (Transaction tx = state.database.beginTx())
        {
            if ( resultOnly )
            {
                writer.emit( out, ResultWalker.result( state ) );
            }
            else
            {
                writer.emit( out, Walker.fullGraph( state.database ) );
            }
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

    private static boolean isABlockOfType( List<String> block, String type )
    {
        if ( block.size() >= 3 )
        {
            String first = block.get( 0 );
            if ( first.startsWith( "[" + type + "]" ) )
            {
                return true;
            }
        }
        return false;
    }

    private static boolean isACommentWith( List<String> block, String command )
    {
        String first = block.get( 0 );
        return first.startsWith( "//" + command ) || first.startsWith( "// " + command );
    }

    private static boolean isCodeBlock( String language, List<String> block )
    {
        String first = block.get( 0 );
        if ( first.charAt( 0 ) != '[' )
        {
            return false;
        }
        if ( first.contains( "source" ) && first.contains( language ) )
        {
            return true;
        }
        if ( block.size() > 4 && first.startsWith( "[[" ) )
        {
            String second = block.get( 1 );
            if ( second.contains( "source" ) && second.contains( language ) )
            {
                return true;
            }
        }
        return false;
    }

    private static List<String> getQueriesBlockContent( Block block )
    {
        List<String> statements = new ArrayList<>();
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
                    if ( line.endsWith( ";" ) )
                    {
                        statements.add( StringUtils.join( queryLines, CypherDoc.EOL ) );
                        queryLines.clear();
                    }
                }
            }
        }
        if ( queryLines.size() > 0 )
        {
            statements.add( StringUtils.join( queryLines, CypherDoc.EOL ) );
        }
        return statements;
    }

    private static String getRawCodeBlockContent( Block block )
    {
        List<String> lines = new ArrayList<>();
        boolean codeStarted = false;
        for ( String line : block.lines )
        {
            if ( !codeStarted )
            {
                if ( line.startsWith( CODE_BLOCK ) )
                {
                    codeStarted = true;
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
                    lines.add( line );
                }
            }
        }
        return StringUtils.join( lines, "\n" );
    }

    private static String executeSql( String sql, Connection sqldb )
    {
        StringBuilder builder = new StringBuilder( 512 );
        try (Statement statement = sqldb.createStatement())
        {
            if ( statement.execute( sql ) )
            {
                try (ResultSet result = statement.getResultSet())
                {
                    ResultSetMetaData meta = result.getMetaData();
                    int rowCount = 0;
                    int columnCount = meta.getColumnCount();
                    String line = new String( new char[columnCount] ).replace( "\0", "+" + LINE_SEGMENT ) + "+\n";
                    builder.append( line );

                    for ( int i = 1; i <= columnCount; i++ )
                    {
                        String output = meta.getColumnLabel( i );
                        printColumn( builder, output );
                    }
                    builder.append( "|\n" ).append( line );
                    while ( result.next() )
                    {
                        rowCount++;
                        for ( int i = 1; i <= columnCount; i++ )
                        {
                            String output = result.getString( i );
                            printColumn( builder, output );
                        }
                        builder.append( "|\n" );
                    }

                    builder.append( line ).append( rowCount ).append( " rows\n" );
                }
            }
        }
        catch ( SQLException sqlException )
        {
            throw new RuntimeException( sqlException );
        }
        return builder.toString();
    }

    private static void printColumn( StringBuilder builder, String value )
    {
        if ( value == null )
        {
            value = "<null>";
        }
        builder.append( "| " ).append( value ).append( SPACE_SEGMENT.substring( value.length() + 1 ) );
    }

    private static String replaceFilename( String query, String filename, String replacement )
    {
        return query.replace( "'" + filename + "'", "'" + replacement + "'" )
                    .replace( '"' + filename + '"', '"' + replacement + '"' );
    }
}
