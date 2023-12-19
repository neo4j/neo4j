/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.console.input;

import java.io.PrintStream;

import org.neo4j.helpers.Args;

public abstract class ArgsCommand implements Command
{
    @Override
    public final void run( String[] args, PrintStream out ) throws Exception
    {
        run( Args.parse( args ), out );
    }

    protected abstract void run( Args args, PrintStream out ) throws Exception;
}
