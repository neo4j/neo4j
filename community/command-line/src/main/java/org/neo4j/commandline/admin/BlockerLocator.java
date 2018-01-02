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
package org.neo4j.commandline.admin;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.neo4j.helpers.Service;

public interface BlockerLocator
{
    Iterable<AdminCommand.Blocker> findBlockers( String name ) throws NoSuchElementException;

    static BlockerLocator fromServiceLocator()
    {
        return commandName ->
        {
            ArrayList<AdminCommand.Blocker> blockers = new ArrayList<>();
            for ( AdminCommand.Blocker blocker : Service.load( AdminCommand.Blocker.class ) )
            {
                if ( blocker.commands().contains( commandName ) )
                {
                    blockers.add( blocker );
                }
            }
            return blockers;
        };
    }
}
