/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Manages the max heap size (-Xmx) JVM argument, suited for use by the neo4j desktop app running inside
 * an .exe built by launch4j.
 */
public class WindowsLaunch4jIniValue implements Value<Integer>
{
    private final File iniFile;
    private final JvmArgumentParser jvmArgumentParser = new JvmArgumentParser();
    private final ListIO listIo;
    
    public WindowsLaunch4jIniValue( Environment environment, ListIO listIo )
    {
        this.listIo = listIo;
        File appFile = environment.getAppFile();
        assert environment.isRunByApp();
        this.iniFile = new File( appFile.getAbsoluteFile().getParentFile(),
                extensionLess( appFile.getName() ) + ".l4j.ini" );
    }

    private String extensionLess( String name )
    {
        int dot = name.lastIndexOf( '.' );
        return dot != -1 ? name.substring( 0, dot ) : name;
    }

    @Override
    public Integer get()
    {
        if ( iniFile != null )
        {
            try
            {
                for ( String line : listIo.read( new ArrayList<String>(), iniFile ) )
                {
                    if ( jvmArgumentParser.isMaxHeap( line ) )
                    {
                        return jvmArgumentParser.parseMaxHeap( line );
                    }
                }
            }
            catch ( IOException e )
            {
                // OK I'd guess
            }
        }
        return jvmArgumentParser.getCurrentMaxHeap();
    }

    private String normalize( String string )
    {
        return string.trim();
    }

    @Override
    public void set( Integer value )
    {
        try
        {
            List<String> lines = new ArrayList<String>();
            ListIterator<String> iterator = listIo.read( lines, iniFile ).listIterator();
            boolean replaced = false;
            String rawValue = jvmArgumentParser.produceMaxHeapArgument( value );
            while ( iterator.hasNext() )
            {
                String item = normalize( iterator.next() );
                if ( jvmArgumentParser.isMaxHeap( item ) )
                {
                    iterator.set( rawValue );
                    replaced = true;
                }
            }
            
            if ( !replaced )
            {
                lines.add( rawValue );
            }
            
            listIo.write( lines, iniFile );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public boolean isWritable()
    {
        return true;
    }
}
