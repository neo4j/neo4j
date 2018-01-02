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
package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.helpers.collection.PrefetchingIterator;

public class CsvFileReader extends PrefetchingIterator<String[]>
{
    private final BufferedReader reader;
    private String delimiter;
    
    public CsvFileReader( File file ) throws IOException
    {
        this( file, null );
    }
    
    public CsvFileReader( File file, String delimiter ) throws IOException
    {
        this.delimiter = delimiter;
        this.reader = new BufferedReader( new FileReader( file ) );
    }
    
    @Override
    protected String[] fetchNextOrNull()
    {
        try
        {
            String line = reader.readLine();
            if ( line == null )
            {
                close();
                return null;
            }
            
            if ( delimiter == null )
            {
                delimiter = figureOutDelimiter( line );
            }
            return line.split( delimiter );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected String figureOutDelimiter( String line )
    {
        String[] candidates = new String[] { "\t", "," };
        for ( String candidate : candidates )
        {
            if ( line.indexOf( candidate ) > -1 )
            {
                return candidate;
            }
        }
        throw new RuntimeException( "Couldn't guess delimiter in '"
                + line + "'" );
    }

    public void close() throws IOException
    {
        reader.close();
    }
}
