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
package org.neo4j.commandline.dbms;

import javax.annotation.Nonnull;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.CommandContext;
import org.neo4j.commandline.arguments.Arguments;

import static org.neo4j.commandline.dbms.DiagnosticsReportCommand.DEFAULT_CLASSIFIERS;

@ServiceProvider
public class DiagnosticsReportCommandProvider implements AdminCommand.Provider
{
    @Nonnull
    @Override
    public String getName()
    {
        return "report";
    }

    @Nonnull
    @Override
    public Arguments allArguments()
    {
        return DiagnosticsReportCommand.allArguments();
    }

    @Nonnull
    @Override
    public String summary()
    {
        return "Produces a zip/tar of the most common information needed for remote assessments.";
    }

    @Nonnull
    @Override
    public AdminCommandSection commandSection()
    {
        return AdminCommandSection.general();
    }

    @Nonnull
    @Override
    public String description()
    {
        return "Will collect information about the system and package everything in an archive. If you specify 'all', " +
                "everything will be included. You can also fine tune the selection by passing classifiers to the tool, " +
                "e.g 'logs tx threads'. For a complete list of all available classifiers call the tool with " +
                "the '--list' flag. If no classifiers are passed, the default list of `" +
                String.join( " ", DEFAULT_CLASSIFIERS ) + "` will be used." ;
    }

    @Nonnull
    @Override
    public AdminCommand create( CommandContext ctx )
    {
        return new DiagnosticsReportCommand( ctx.getHomeDir(), ctx.getConfigDir(), ctx.getOutsideWorld() );
    }
}
