/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.shell.apps;

import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.App;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Wraps a {@link org.neo4j.util.shell.apps.extra.Gsh} in a {@link NeoApp} to be
 * wrapped in a transaction among other things.
 */
public class Gsh extends NeoApp
{
    private App sh = new org.neo4j.util.shell.apps.extra.Gsh();

    @Override
    public String getDescription()
    {
        return this.sh.getDescription();
    }

    @Override
    public String getDescription( String option )
    {
        return this.sh.getDescription( option );
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
        throws ShellException
    {
        return sh.execute( parser, session, out );
    }
}
