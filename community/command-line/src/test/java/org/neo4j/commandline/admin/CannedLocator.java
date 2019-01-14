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
package org.neo4j.commandline.admin;

import java.util.HashMap;
import java.util.Map;

class CannedLocator implements CommandLocator
{
    private final Map<String,AdminCommand.Provider> commands;

    CannedLocator( AdminCommand.Provider... commands )
    {
        this.commands = new HashMap<>();
        for ( AdminCommand.Provider provider : commands )
        {
            this.commands.put( provider.name(), provider );
        }
    }

    @Override
    public AdminCommand.Provider findProvider( String s )
    {
        return commands.get( s );
    }

    @Override
    public Iterable<AdminCommand.Provider> getAllProviders()
    {
        return commands.values();
    }
}
