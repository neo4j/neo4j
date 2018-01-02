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
package org.neo4j.helpers.collection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @deprecated This class will be removed in the next major release.
 */
@Deprecated
public class LinesOfFileIterator extends PrefetchingIterator<String> implements ClosableIterator<String>
{
    private final BufferedReader reader;
    private boolean closed;
    
    public LinesOfFileIterator( File file, String encoding ) throws IOException
    {
        try
        {
            reader = new BufferedReader( new InputStreamReader(new FileInputStream(file), encoding) );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Override
    protected String fetchNextOrNull()
    {
        if ( closed ) return null;
        try
        {
            String line = reader.readLine();
            if ( line == null ) close();
            return line;
        }
        catch ( IOException e )
        {
            close();
            throw new RuntimeException( e );
        }
    }

    public void close()
    {
        if ( closed ) return;
        try
        {
            reader.close();
        }
        catch ( IOException e )
        {   // Couldn't close
            e.printStackTrace();
        }
        finally
        {
            closed = true;
        }
    }
}
