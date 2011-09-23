/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;

public class DumpLogicalLog extends org.neo4j.kernel.impl.util.DumpLogicalLog
{
    public static void main( String[] args ) throws IOException
    {
        for ( String arg : args )
        {
            int dumped = new DumpLogicalLog().dump( arg );
            if ( dumped == 0 && isAGraphDatabaseDirectory( arg ) )
            {   // If none were found and we really pointed to a neodb directory
                // then go to its index folder and try there.
                new DumpLogicalLog().dump( new File( arg, "index" ).getAbsolutePath() );
            }
        }
    }

    @Override
    protected XaCommandFactory instantiateCommandFactory()
    {
        return new CommandFactory();
    }

    @Override
    protected String getLogPrefix()
    {
        return "lucene.log";
    }
    
    private static class CommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer )
                throws IOException
        {
            return LuceneCommand.readCommand( byteChannel, buffer, null );
        }
    }
}
