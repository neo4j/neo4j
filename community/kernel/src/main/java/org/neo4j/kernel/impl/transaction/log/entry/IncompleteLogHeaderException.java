/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.File;
import java.io.IOException;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

/**
 * Used to signal an incomplete log header, i.e. if file is smaller than the header.
 * This exception is still an {@link IOException}, but a specific subclass of it as to make possible
 * special handling.
 */
public class IncompleteLogHeaderException extends IOException
{
    public IncompleteLogHeaderException( File file, int readSize )
    {
        super( template( file, readSize ) );
    }

    public IncompleteLogHeaderException( int readSize )
    {
        super( template( null, readSize ) );
    }

    private static String template( File file, int readSize )
    {
        StringBuilder builder = new StringBuilder( "Unable to read log version and last committed tx" );
        if ( file != null )
        {
            builder.append( " from '" ).append( file.getAbsolutePath() ).append( '\'' );
        }
        builder.append( ". Was only able to read " ).append( readSize ).append( " bytes, but was expecting " )
               .append( LOG_HEADER_SIZE );
        return builder.toString();
    }
}
