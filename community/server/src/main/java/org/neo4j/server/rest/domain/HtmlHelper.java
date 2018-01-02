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
package org.neo4j.server.rest.domain;

import java.util.Collection;
import java.util.Map;

/**
 * This is just a simple test of how a HTML renderer could be like
 */
public class HtmlHelper
{
    private final static String STYLE_LOCATION = "http://resthtml.neo4j.org/style/";
    private final static String HTML_JAVASCRIPT_LOCATION = "/webadmin/htmlbrowse.js";

    public static String from( final Object object, final ObjectType objectType )
    {
        StringBuilder builder = start( objectType, null );
        append( builder, object, objectType );
        return end( builder );
    }

    public static StringBuilder start( final ObjectType objectType, final String additionalCodeInHead )
    {
        return start( objectType.getCaption(), additionalCodeInHead );
    }

    public static StringBuilder start( final String title, final String additionalCodeInHead )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n" );
        builder.append( "<html><head><title>" + title + "</title>" );
        if ( additionalCodeInHead != null )
        {
            builder.append( additionalCodeInHead );
        }
        builder.append( "<meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\">\n" + "<link href='"
                        + STYLE_LOCATION + "rest.css' rel='stylesheet' type='text/css'>\n"
                        + "<script type='text/javascript' src='" + HTML_JAVASCRIPT_LOCATION + "'></script>\n"
                        + "</head>\n<body onload='javascript:neo4jHtmlBrowse.start();' id='" + title.toLowerCase()
                        + "'>\n" + "<div id='content'>" + "<div id='header'>"
                        + "<h1><a title='Neo4j REST interface' href='/'><span>Neo4j REST interface</span></a></h1>"
                        + "</div>" + "\n<div id='page-body'>\n" );
        return builder;
    }

    public static String end( final StringBuilder builder )
    {
        builder.append( "<div class='break'>&nbsp;</div>" + "</div></div></body></html>" );
        return builder.toString();
    }

    public static void appendMessage( final StringBuilder builder, final String message )
    {
        builder.append( "<p class=\"message\">" + message + "</p>" );
    }

    public static void append( final StringBuilder builder, final Object object, final ObjectType objectType )
    {
        if ( object instanceof Collection )
        {
            builder.append( "<ul>\n" );
            for ( Object item : (Collection<?>) object )
            {
                builder.append( "<li>" );
                append( builder, item, objectType );
                builder.append( "</li>\n" );
            }
            builder.append( "</ul>\n" );
        }
        else if ( object instanceof Map )
        {
            Map<?, ?> map = (Map<?, ?>) object;
            String htmlClass = objectType.getHtmlClass();
            String caption = objectType.getCaption();
            if ( !map.isEmpty() )
            {
                boolean isNodeOrRelationship = ObjectType.NODE.equals( objectType )
                                               || ObjectType.RELATIONSHIP.equals( objectType );
                if ( isNodeOrRelationship )
                {
                    builder.append( "<h2>" + caption + "</h2>\n" );
                    append( builder, map.get( "data" ), ObjectType.PROPERTIES );
                    htmlClass = "meta";
                    caption += " info";
                }
                if ( ObjectType.NODE.equals( objectType ) && map.size() == 1 )
                {
                    // there's only properties, so we're finished here
                    return;
                }
                builder.append( "<table class=\"" + htmlClass + "\"><caption>" );
                builder.append( caption );
                builder.append( "</caption>\n" );
                boolean odd = true;
                for ( Map.Entry<?, ?> entry : map.entrySet() )
                {
                    if ( isNodeOrRelationship && "data".equals( entry.getKey() ) )
                    {
                        continue;
                    }
                    builder.append( "<tr" + ( odd ? " class='odd'" : "" ) + ">" );
                    odd = !odd;
                    builder.append( "<th>" + entry.getKey() + "</th><td>" );
                    // TODO We always assume that an inner map is for
                    // properties, correct?
                    append( builder, entry.getValue(), ObjectType.PROPERTIES );
                    builder.append( "</td></tr>\n" );
                }
                builder.append( "</table>\n" );
            }
            else
            {
                builder.append( "<table class=\"" + htmlClass + "\"><caption>" );
                builder.append( caption );
                builder.append( "</caption>" );
                builder.append( "<tr><td></td></tr>" );
                builder.append( "</table>" );
            }
        }
        else
        {
            builder.append( object != null ? embedInLinkIfClickable( object.toString() ) : "" );
        }
    }

    private static String embedInLinkIfClickable( String string )
    {
        // TODO Hardcode "http://" string?
        if ( string.startsWith( "http://" ) || string.startsWith( "https://" ) )
        {
            String anchoredString = "<a href=\"" + string + "\"";

            // TODO Hardcoded /node/, /relationship/ string?
            String anchorClass = null;
            if ( string.contains( "/node/" ) )
            {
                anchorClass = "node";
            }
            else if ( string.contains( "/relationship/" ) )
            {
                anchorClass = "relationship";
            }
            if ( anchorClass != null )
            {
                anchoredString += " class=\"" + anchorClass + "\"";
            }
            anchoredString += ">" + escapeHtml( string ) + "</a>";
            string = anchoredString;
        }
        else
        {
            string = escapeHtml( string );
        }
        return string;
    }

    private static String escapeHtml( final String string )
    {
        if ( string == null )
        {
            return null;
        }
        String res = string.replace( "&", "&amp;" );
        res = res.replace( "\"", "&quot;" );
        res = res.replace( "<", "&lt;" );
        res = res.replace( ">", "&gt;" );
        return res;
    }

    public static enum ObjectType
    {
        NODE,
        RELATIONSHIP,
        PROPERTIES,
        ROOT,
        INDEX_ROOT,

        ;

        String getCaption()
        {
            return name().substring( 0, 1 )
                    .toUpperCase() + name().substring( 1 )
                    .toLowerCase();
        }

        String getHtmlClass()
        {
            return getCaption().toLowerCase();
        }
    }
}
