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

import org.apache.commons.lang3.StringUtils;

public class OutputHelper
{
    private static final String PASSTHROUGH_BLOCK = "++++";

    static String passthroughMarker( String role, String htmlElement, String docbookElement )
    {
        StringBuilder sb = new StringBuilder( 64 );
        sb.append( '<' )
                .append( htmlElement )
                .append( " class=\"" )
                .append( role )
                .append( "\"></" )
                .append( htmlElement )
                .append( '>' );
        String html = sb.toString();
        sb = new StringBuilder( 64 );
        sb.append( '<' )
                .append( docbookElement )
                .append( " role=\"" )
                .append( role )
                .append( "\"></" )
                .append( docbookElement )
                .append( '>' );
        String docbook = sb.toString();
        return passthroughHtmlAndDocbook( html, docbook );
    }

    private static String passthroughHtml( String html )
    {
        return OutputHelper.passthroughWithCondition(
                "ifdef::backend-html,backend-html5,backend-xhtml11,backend-deckjs[]", html );
    }

    private static String passthroughDocbook( String docbook )
    {
        return OutputHelper.passthroughWithCondition(
                "ifndef::backend-html,backend-html5,backend-xhtml11,backend-deckjs[]", docbook );
    }

    private static String passthroughHtmlAndDocbook( String html, String docbook )
    {
        return passthroughHtml( html ) + CypherDoc.EOL + passthroughDocbook( docbook ) + CypherDoc.EOL;
    }

    private static String passthroughWithCondition( String condition, String content )
    {
        return StringUtils.join(
                new String[] { condition, OutputHelper.PASSTHROUGH_BLOCK, content, OutputHelper.PASSTHROUGH_BLOCK, "endif::[]" }, CypherDoc.EOL );
    }
}
