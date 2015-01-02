/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.metrics;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class CSVFile
    implements Closeable
{
    private final OutputStream out;

    public CSVFile(OutputStream outStream, Iterable<String> columns)
            throws IOException
    {
        this.out = outStream;

        // Print header
        StringBuilder builder = new StringBuilder(  );
        String separator = "";
        for ( String column : columns )
        {
            builder.append( separator );
            builder.append( column );
            separator = "\t";
        }
        builder.append( '\n' );
        out.write( builder.toString().getBytes( Charset.forName( "UTF-8" ) ) );
    }

    public void print(Number... values) throws IOException
    {
        StringBuilder builder = new StringBuilder(  );
        String separator = "";
        for ( Number value : values )
        {
            builder.append( separator );
            builder.append( value.longValue() );
            separator = "\t";
        }
        builder.append( '\n' );
        out.write( builder.toString().getBytes( Charset.forName( "UTF-8" ) ) );
    }

    @Override
    public void close() throws IOException
    {
        out.close();
    }
}
