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
package org.neo4j.shell.kernel.apps.cypher;

import java.io.PrintWriter;
import java.rmi.RemoteException;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.cypher.export.SubGraphExporter;
import org.neo4j.shell.Output;
import org.neo4j.shell.OutputAsWriter;
import org.neo4j.shell.ShellException;

public class Exporter
{
    private final SubGraphExporter exporter;

    Exporter(SubGraph graph)
    {
        exporter = new SubGraphExporter( graph );
    }

    public void export( Output out ) throws RemoteException, ShellException
    {
        begin( out );
        exporter.export(asWriter(out));
        out.println(";");
        commit(out);
    }

    private PrintWriter asWriter(Output out) {
        return new PrintWriter( new OutputAsWriter( out ) );
    }

    private void begin( Output out ) throws RemoteException
    {
        out.println( "begin" );
    }

    private void commit( Output out ) throws RemoteException
    {
        out.println( "commit" );
    }
}
