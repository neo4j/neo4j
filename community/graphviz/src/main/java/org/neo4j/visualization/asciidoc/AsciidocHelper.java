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
package org.neo4j.visualization.asciidoc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle;
import org.neo4j.visualization.graphviz.AsciiDocStyle;
import org.neo4j.visualization.graphviz.GraphStyle;
import org.neo4j.visualization.graphviz.GraphvizWriter;
import org.neo4j.walk.Walker;

import static java.lang.String.format;

public class AsciidocHelper
{
    /**
     * Cut to max 123 chars for PDF layout compliance. Or even less, for
     * readability.
     */
    private static final int MAX_CHARS_PER_LINE = 100;
    /**
     * Cut text message line length for readability.
     */
    private static final int MAX_TEXT_LINE_LENGTH = 80;
    /**
     * Characters to remove from the title.
     */
    private static final String ILLEGAL_STRINGS = "[:\\(\\)\t;&/\\\\]";

    public static String createGraphViz( String title,
                                         GraphDatabaseService graph, String identifier )
    {
        return createGraphViz( title, graph, identifier,
                AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors() );
    }

    public static String createGraphViz( String title,
                                         GraphDatabaseService graph, String identifier,
                                         String graphvizOptions )
    {
        return createGraphViz( title, graph, identifier,
                AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors(),
                graphvizOptions );
    }

    public static String createGraphVizWithNodeId( String title,
                                                   GraphDatabaseService graph, String identifier )
    {
        return createGraphViz( title, graph, identifier,
                AsciiDocStyle.withAutomaticRelationshipTypeColors() );
    }

    public static String createGraphVizWithNodeId( String title,
                                                   GraphDatabaseService graph, String identifier,
                                                   String graphvizOptions )
    {
        return createGraphViz( title, graph, identifier,
                AsciiDocStyle.withAutomaticRelationshipTypeColors(),
                graphvizOptions );
    }

    public static String createGraphVizDeletingReferenceNode( String title,
                                                              GraphDatabaseService graph, String identifier )
    {
        return createGraphVizDeletingReferenceNode( title, graph, identifier,
                "" );
    }

    public static String createGraphVizDeletingReferenceNode( String title,
                                                              GraphDatabaseService graph, String identifier,
                                                              String graphvizOptions )
    {
        return createGraphViz( title, graph, identifier,
                AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors(),
                graphvizOptions );
    }

    public static String createGraphVizWithNodeIdDeletingReferenceNode(
            String title, GraphDatabaseService graph, String identifier )
    {
        return createGraphVizWithNodeIdDeletingReferenceNode( title, graph,
                identifier, "" );
    }

    public static String createGraphVizWithNodeIdDeletingReferenceNode(
            String title, GraphDatabaseService graph, String identifier,
            String graphvizOptions )
    {
        return createGraphViz( title, graph, identifier,
                AsciiDocStyle.withAutomaticRelationshipTypeColors(),
                graphvizOptions );
    }

    /**
     * Create graphviz output using a {@link GraphStyle}.
     * {@link GraphStyle} is implemented by {@link AsciiDocSimpleStyle} and {@link AsciiDocStyle}.
     * {@link AsciiDocSimpleStyle} provides different customization options for coloring.
     * 
     * @param title the title of the visualization
     * @param graph the database to use
     * @param identifier the identifier to include in the filename
     * @param graphStyle the style configuration to use
     * @return a string to be included in an AsciiDoc document
     */
    public static String createGraphViz( String title,
                                         GraphDatabaseService graph, String identifier, GraphStyle graphStyle )
    {
        return createGraphViz( title, graph, identifier, graphStyle, "" );
    }

    public static String createGraphViz( String title,
                                         GraphDatabaseService graph, String identifier,
                                         GraphStyle graphStyle, String graphvizOptions )
    {
        try ( Transaction tx = graph.beginTx() )
        {
            GraphvizWriter writer = new GraphvizWriter( graphStyle );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try
            {
                writer.emit( out, Walker.fullGraph( graph ) );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }

            String safeTitle = title.replaceAll( ILLEGAL_STRINGS, "" );

            tx.success();

            try
            {
                return "." + title + "\n[\"dot\", \""
                        + (safeTitle + "-" + identifier).replace( " ", "-" )
                        + ".svg\", \"neoviz\", \"" + graphvizOptions + "\"]\n"
                        + "----\n" + out.toString( "UTF-8" ) + "----\n";
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    public static String createOutputSnippet( final String output )
    {
        return "[source]\n----\n" + output + "\n----\n";
    }

    public static String createQueryResultSnippet( final String output )
    {
        return "[queryresult]\n----\n" + output
                + (output.endsWith( "\n" ) ? "" : "\n") + "----\n";
    }

    public static String createQueryFailureSnippet( final String output )
    {
        return "[source]\n----\n" + wrapText( output ) + "\n----\n";
    }

    private static String wrapText( final String text )
    {
        return wrap( text, MAX_TEXT_LINE_LENGTH, " ", "\n" );
    }

    static String wrapQuery( final String query )
    {
        String wrapped = wrap( query, MAX_CHARS_PER_LINE, ", ", ",\n  " );
        wrapped = wrap( wrapped, MAX_CHARS_PER_LINE, "),(", "),\n  (" );
        return wrap( wrapped, MAX_CHARS_PER_LINE, " ", "\n  " );
    }

    private static String wrap( final String text, final int maxChars, final String search, final String replace )
    {
        StringBuffer out = new StringBuffer( text.length() + 10 * replace.length() );
        String pattern = Pattern.quote( search );
        for ( String line : text.trim()
                .split( "\n" ) )
        {
            if ( line.length() < maxChars )
            {
                out.append( line )
                        .append( '\n' );
            }
            else
            {
                int currentLength = 0;
                for ( String word : line.split( pattern ) )
                {
                    if ( currentLength + word.length() > maxChars )
                    {
                        if ( currentLength > 0 )
                        {
                            out.append( replace );
                        }
                        out.append( word );
                        currentLength = replace.length() + word.length();
                    }
                    else
                    {
                        if ( currentLength != 0 )
                        {
                            out.append( search );
                            currentLength += search.length();
                        }
                        out.append( word );
                        currentLength += word.length();
                    }
                }
                out.append( '\n' );
            }
        }
        return out.substring( 0, out.length() - 1 );
    }

    public static String createCypherSnippet( final String query )
    {
        String[] keywordsToBreakOn = new String[]{"start", "create", "unique", "set", "delete", "foreach",
                "match", "where", "with", "return", "skip", "limit", "order by", "asc", "ascending",
                "desc", "descending", "create", "remove", "drop", "using", "merge", "assert", "constraint"};

        String[] unbreakableKeywords = new String[]{"label", "values", "on", "index"};
        return createLanguageSnippet( query, "cypher", keywordsToBreakOn, unbreakableKeywords );
    }

    public static String createSqlSnippet( final String query )
    {
        return createAsciiDocSnippet( "sql", query );
    }

    private static String createLanguageSnippet( String query,
                                                 String language,
                                                 String[] keywordsToBreakOn,
                                                 String[] unbreakableKeywords )
    {
        // This is not something I'm proud of. This should be done in Cypher by the parser.
        // For now, learn to live with it.
        String formattedQuery;
        if ( "cypher".equals( language ) && query.contains( "merge" ) )
        {
            formattedQuery = uppercaseKeywords( query, keywordsToBreakOn );
            formattedQuery = uppercaseKeywords( formattedQuery, unbreakableKeywords );
        } else
        {
            formattedQuery = breakOnKeywords( query, keywordsToBreakOn );
            formattedQuery = uppercaseKeywords( formattedQuery, unbreakableKeywords );
        }
        String result = createAsciiDocSnippet(language, formattedQuery);
        return limitChars( result );
    }

    public static String createCypherSnippetFromPreformattedQuery( final String formattedQuery, boolean executable )
    {
        return cypherSnippet( formattedQuery, ( executable ? "cypher" : "cypher-noexec" ) );
    }

    private static String cypherSnippet( final String formattedQuery, final String lang )
    {
        return format( "[source,%s]\n----\n%s%s----\n", lang, wrapQuery( formattedQuery ),
                formattedQuery.endsWith( "\n" ) ? "" : "\n" );
    }

    public static String createAsciiDocSnippet(String language, String formattedQuery)
    {
        return format("[source,%s]\n----\n%s%s----\n",
                language, formattedQuery, formattedQuery.endsWith("\n") ? "" : "\n");
    }

    private static String breakOnKeywords( String query, String[] keywordsToBreakOn )
    {
        String result = query;
        for ( String keyword : keywordsToBreakOn )
        {
            String upperKeyword = keyword.toUpperCase();

            result = ucaseIfFirstInLine( result, keyword, upperKeyword );

            result = result.
                    replace( " " + upperKeyword + " ", "\n" + upperKeyword + " " );
        }
        return result;
    }

    private static String ucaseIfFirstInLine( String result, String keyword, String upperKeyword )
    {
        if ( result.length() > keyword.length() && result.startsWith( keyword + " " ) )
        {
            result = upperKeyword + " " + result.substring( keyword.length() + 1 );
        }

        return result.replace( "\n" + keyword, "\n" + upperKeyword );
    }

    private static String uppercaseKeywords( String query, String[] uppercaseKeywords )
    {
        String result = query;
        for ( String keyword : uppercaseKeywords )
        {
            String upperKeyword = keyword.toUpperCase();

            result = ucaseIfFirstInLine( result, keyword, upperKeyword );

            result = result.
                    replace( " " + keyword + " ", " " + upperKeyword + " " );
        }
        return result;
    }

    private static String limitChars( String result )
    {
        String[] lines = result.split( "\n" );
        String finalRes = "";
        for ( String line : lines )
        {
            line = line.trim();
            if ( line.length() > MAX_CHARS_PER_LINE )
            {
                line = line.replaceAll( ", ", ",\n      " );
            }
            finalRes += line + "\n";
        }
        return finalRes;
    }
}
