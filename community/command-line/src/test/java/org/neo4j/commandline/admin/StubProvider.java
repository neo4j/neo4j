/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.nio.file.Path;

import org.neo4j.commandline.arguments.Arguments;

class StubProvider extends AdminCommand.Provider
{
    private final String summary;
    private AdminCommandSection general;

    StubProvider( String name, String summary, AdminCommandSection section )
    {
        super( name );
        this.summary = summary;
        this.general = section;
    }

    @Override
    public Arguments allArguments()
    {
        return Arguments.NO_ARGS;
    }

    @Override
    public String description()
    {
        return "description";
    }

    @Override
    public String summary()
    {
        return summary;
    }

    @Override
    public AdminCommandSection commandSection()
    {
        return general;
    }

    @Override
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
