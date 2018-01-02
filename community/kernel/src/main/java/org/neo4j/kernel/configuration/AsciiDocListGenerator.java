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
package org.neo4j.kernel.configuration;

import java.util.List;

import org.neo4j.helpers.Triplet;

/**
 * Generate a table with links to more details. For PDF generation, a list is
 * used instead of a table.
 */
public class AsciiDocListGenerator
{
    private String listId;
    private String title;
    private boolean shortenDescription;
    
    public AsciiDocListGenerator( String listId, String title, boolean shortenDescription )
    {
        this.listId = listId;
        this.title = title;
        this.shortenDescription = shortenDescription;
    }

    public String generateListAndTableCombo( List<Triplet<String, String, String>> items )
    {
        StringBuilder sb = new StringBuilder( 200 * items.size() );
        StringBuilder print = new StringBuilder( 100 * items.size() );
        if ( listId != null )
        {
            sb.append( "[[" ).append( listId ).append( "]]\n" );
        }
        if ( title != null )
        {
            sb.append( '.' ).append( title ).append( '\n' );
        }
        sb.append( ConfigAsciiDocGenerator.IFDEF_HTMLOUTPUT ).append( '\n' )
          .append( "[options=\"header\"]\n" )
          .append( "|===\n" )
          .append( "|Name|Description\n" );
        print.append( ConfigAsciiDocGenerator.IFDEF_NONHTMLOUTPUT ).append( '\n' );
        for ( Triplet<String,String,String> item : items )
        {
            String id = item.first();
            String name = item.second();
            String description = item.third();
            if ( shortenDescription )
            {
                int pos = description.indexOf( ". " );
                if  ( pos == -1 )
                {
                    pos = description.indexOf( "; " );
                }
                if ( pos > 10 )
                {
                    description = description.substring( 0, pos );
                }
            }
            sb.append( "|<<" )
                .append( id )
                .append( ',' )
                .append( name )
                .append( ">>|" )
                .append( description );

            print.append( "* <<" )
                .append( id )
                .append( ',' )
                .append( name )
                .append( ">>: " )
                .append( description );

            if ( !description.endsWith( "." ) )
            {
                sb.append( '.' );
                print.append( '.' );
            }
            sb.append( '\n' );
            print.append( '\n' );
        }
        sb.append( "|===\n" )
            .append( ConfigAsciiDocGenerator.ENDIF );
        print.append( ConfigAsciiDocGenerator.ENDIF )
            .append( '\n' );
        sb.append( print.toString() );
        return sb.toString();
    }
}
