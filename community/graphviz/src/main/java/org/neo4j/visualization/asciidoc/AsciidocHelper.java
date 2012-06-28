/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.visualization.asciidoc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle;
import org.neo4j.visualization.graphviz.AsciiDocStyle;
import org.neo4j.visualization.graphviz.GraphStyle;
import org.neo4j.visualization.graphviz.GraphvizWriter;
import org.neo4j.walk.Walker;

public class AsciidocHelper
{
    /**
     * Cut to max 123 chars for PDF layout compliance.
     */
    private static final int MAX_CHARS_PER_LINE = 123;
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
        removeReferenceNode( graph );
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
        removeReferenceNode( graph );
        return createGraphViz( title, graph, identifier,
                AsciiDocStyle.withAutomaticRelationshipTypeColors(),
                graphvizOptions );
    }

    /**
     * Create graphviz output using a {@link GraphStyle) which is implemented by
     * {@link AsciiDocSimpleStyle} and {@link AsciiDocStyle}.
     * {@link AsciiDocSimpleStyle} provides different customization options for
     * coloring.
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
        GraphvizWriter writer = new GraphvizWriter( graphStyle );
        OutputStream out = new ByteArrayOutputStream();
        try
        {
            writer.emit( out, Walker.fullGraph( graph ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

        String safeTitle = title.replaceAll( ILLEGAL_STRINGS, "" );

        return "." + title + "\n[\"dot\", \""
               + ( safeTitle + "-" + identifier ).replace( " ", "-" )
               + ".svg\", \"neoviz\", \"" + graphvizOptions + "\"]\n"
               + "----\n" + out.toString() + "----\n";
    }

    private static void removeReferenceNode( GraphDatabaseService graph )
    {
        Transaction tx = graph.beginTx();
        try
        {
            graph.getReferenceNode().delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public static String createOutputSnippet( final String output )
    {
        return "[source]\n----\n" + output + "\n----\n";
    }

    public static String createQueryResultSnippet( final String output )
    {
        return "[queryresult]\n----\n" + output + "\n----\n";
    }

    public static String createCypherSnippet( final String query )
    {
        String[] keywordsToBreakOn = new String[] {"start", "create", "set", "delete", "foreach",
        "match", "where", "with", "return", "skip", "limit", "order by", "asc", "ascending",
        "desc", "descending", "relate"};
        return createLanguageSnippet( query, "cypher", keywordsToBreakOn );
    }

    public static String createSqlSnippet( final String query )
    {
        String[] keywordsToBreakOn = new String[] { "select", "from", "where",
                "skip", "limit", "order by", "asc", "ascending", "desc",
                "descending", "join", "group by" };
        return createLanguageSnippet( query, "sql", keywordsToBreakOn );
    }

    private static String createLanguageSnippet( String query,
            String language, String[] keywordsToBreakOn )
    {
        String formattedQuery = breakOnKeywords( query, keywordsToBreakOn );
        String result = "[source," + language + "]\n----\n" + formattedQuery
                        + "\n----\n";
        return limitChars( result );
    }

    private static String breakOnKeywords( String query,
            String[] keywordsToBreakOn )
    {
        String result = query;
        for ( String keyword : keywordsToBreakOn )
        {
            String upperKeyword = keyword.toUpperCase();
            result = result.
                    replace(keyword+" ", upperKeyword+" ").
                    replace(keyword+" ", upperKeyword+" ").
                    replace(" " + upperKeyword + " ", "\n" + upperKeyword + " ");
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
            if (line.length() > MAX_CHARS_PER_LINE )
            {
                line = line.replaceAll( ", ", ",\n      " );
            }
            finalRes += line + "\n";
        }
        return finalRes;
    }
}
