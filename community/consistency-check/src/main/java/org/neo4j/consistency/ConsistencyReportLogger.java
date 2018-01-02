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
package org.neo4j.consistency;

import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;
import org.neo4j.logging.AbstractPrintWriterLogger;
import org.neo4j.logging.Logger;

import java.io.PrintWriter;

public class ConsistencyReportLogger extends AbstractPrintWriterLogger
{
    private final String prefix;

    public ConsistencyReportLogger( Supplier<PrintWriter> writerSupplier, Object lock, String prefix, boolean autoFlush )
    {
        super( writerSupplier, lock, autoFlush );
        this.prefix = prefix;
    }

    @Override
    protected void writeLog( PrintWriter out, String message )
    {
        out.write( prefix );
        out.write( ": " );
        out.write( message );
        out.println();
    }

    @Override
    protected void writeLog( PrintWriter out, String message, Throwable throwable )
    {
        out.write( prefix );
        out.write( ": " );
        out.write( message );
        out.write( ' ' );
        out.write( throwable.getMessage() );
        out.println();
        throwable.printStackTrace( out );
    }

    @Override
    protected Logger getBulkLogger( PrintWriter out, Object lock )
    {
        return new ConsistencyReportLogger( Suppliers.singleton( out ), lock, prefix, false );
    }
}
