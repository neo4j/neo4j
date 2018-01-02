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
package org.neo4j.server.rest.management.console;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.Database;
import org.neo4j.server.webadmin.console.ConsoleSessionCreator;
import org.neo4j.server.webadmin.console.ScriptSession;

public class ShellSessionCreator implements ConsoleSessionCreator
{
    public static final String NAME = "SHELL";
    
    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public ScriptSession newSession( Database database, CypherExecutor cypherExecutor, HttpServletRequest request, LogProvider logProvider )
    {
        return new ShellSession( database.getGraph() );
    }
}
