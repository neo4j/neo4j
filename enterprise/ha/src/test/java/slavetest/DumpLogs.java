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
package slavetest;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.kernel.impl.util.DumpLogicalLog;

public class DumpLogs
{
    public static void main( String[] args ) throws Exception
    {
        for ( File file : sorted( new File( "/home/mattias/dev/svn.neo4j.org/qa/ha-robustness/data/db/1" ).listFiles() ) )
        {
//            if ( !file.getName().contains( ".1" ) ) continue;
            
            try
            {
                if ( file.getName().contains( "logical" ) && !file.getName().endsWith( ".active" ) )
                {
                    System.out.println( "\n=== " + file.getPath() + " ===" );
                    DumpLogicalLog.main( new String[] { file.getPath() } );
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        }
    }

    private static File[] sorted( File[] listFiles )
    {
        Arrays.sort( listFiles, new Comparator<File>()
        {
            @Override
            public int compare( File o1, File o2 )
            {
                return o1.getName().compareTo( o2.getName() );
            }
        } );
        return listFiles;
    }
}
