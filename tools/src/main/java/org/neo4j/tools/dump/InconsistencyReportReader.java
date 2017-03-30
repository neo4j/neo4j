/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tools.dump;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.tools.dump.inconsistency.Inconsistencies;

/**
 * Reads CC inconsistency reports. Example of entry:
 *
 * <pre>
 * ERROR: The referenced relationship record is not in use.
 *     Node[3496089,used=true,rel=14833798,prop=13305361,labels=Inline(0x1000000006:[6]),light,secondaryUnitId=-1]
 *     Inconsistent with: Relationship[14833798,used=false,source=0,target=0,type=0,sPrev=0,sNext=0,tPrev=0,tNext=0,prop=0,secondaryUnitId=-1,!sFirst,!tFirst]
 * </pre>
 */
public class InconsistencyReportReader
{
    private final Inconsistencies inconsistencies;

    public InconsistencyReportReader( Inconsistencies inconsistencies )
    {
        this.inconsistencies = inconsistencies;
    }

    public void read( File file ) throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( file )) )
        {
            read( reader );
        }
    }

    public void read( BufferedReader bufferedReader ) throws IOException
    {
        int state = 0; // 0:inconsistency description, 1:entity, 2:inconsistent with
        String line;
        while ( (line = bufferedReader.readLine()) != null )
        {
            line = line.trim();
            if ( state == 0 )
            {
                if ( !line.contains( " ERROR " ) && !line.contains( " WARNING " ) )
                {
                    continue;
                }
            }
            else if ( state == 1 )
            {
                tryPropagate( line );
            }
            else if ( state == 2 )
            {
                if ( line.startsWith( "Inconsistent with: " ) )
                {
                    line = line.substring( "Inconsistent with: ".length() );
                }

                tryPropagate( line );
            }
            state = (state + 1) % 3;
        }
    }

    private void tryPropagate( String line )
    {
        String entityType = entityType( line );
        long id = id( line );
        if ( entityType != null && id != -1 )
        {
            propagate( entityType, id );
        }
    }

    private void propagate( String entityType, long id )
    {
        switch ( entityType )
        {
        case "Relationship":
            inconsistencies.relationship( id );
            break;
        case "Node":
            inconsistencies.node( id );
            break;
        case "Property":
            inconsistencies.property( id );
            break;
        case "RelationshipGroup":
            inconsistencies.relationshipGroup( id );
            break;
        case "IndexRule":
            inconsistencies.schemaIndex( id );
            break;
        case "IndexEntry":
            inconsistencies.node( id );
            break;
        default:
            // it's OK, we just haven't implemented support for this yet
        }
    }

    private long id( String line )
    {
        int bracket = line.indexOf( '[' );
        if ( bracket > -1 )
        {
            int separator = min( getSeparatorIndex( ',', line, bracket ),
                    getSeparatorIndex( ';', line, bracket ),
                    getSeparatorIndex( ']', line, bracket ) );
            int equally = line.indexOf( "=", bracket );
            int startPosition = (isNotPlainId( bracket, separator, equally ) ? equally : bracket) + 1;
            if ( separator > -1 )
            {
                return Long.parseLong( line.substring( startPosition, separator ) );
            }
        }
        return -1;
    }

    private static int min(int... values)
    {
        int min = Integer.MAX_VALUE;
        for ( int value : values )
        {
            min = Math.min( min, value );
        }
        return min;
    }

    private int getSeparatorIndex( char character, String line, int bracket )
    {
        int index = line.indexOf( character, bracket );
        return index >= 0 ? index : Integer.MAX_VALUE;
    }

    private boolean isNotPlainId( int bracket, int comma, int equally )
    {
        return (equally > bracket) && (equally < comma);
    }

    private String entityType( String line )
    {
        int bracket = line.indexOf( '[' );
        return bracket == -1 ? null : line.substring( 0, bracket );
    }
}
