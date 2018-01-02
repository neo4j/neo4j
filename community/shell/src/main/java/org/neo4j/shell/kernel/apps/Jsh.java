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
package org.neo4j.shell.kernel.apps;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;

/**
 * Wraps a {@link org.neo4j.shell.apps.extra.Jsh} in a
 * {@link TransactionProvidingApp} to be wrapped in a transaction among other things.
 */
@Service.Implementation( App.class )
public class Jsh extends TransactionProvidingApp
{
    private App sh = new org.neo4j.shell.apps.extra.Jsh();

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
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
        throws Exception
    {
        return sh.execute( parser, session, out );
    }
}
