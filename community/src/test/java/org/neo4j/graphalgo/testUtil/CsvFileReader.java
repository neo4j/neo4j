package org.neo4j.graphalgo.testUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.commons.iterator.PrefetchingIterator;

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
