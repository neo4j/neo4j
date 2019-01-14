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

import java.nio.file.Path;
import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

import static java.lang.String.format;

public class MemoryRecommendationsCommandProvider extends AdminCommand.Provider
{
    public MemoryRecommendationsCommandProvider()
    {
        super( "memrec" );
    }

    @Nonnull
    @Override
    public Arguments allArguments()
    {
        return MemoryRecommendationsCommand.buildArgs();
    }

    @Nonnull
    @Override
    public String summary()
    {
        return "Print Neo4j heap and pagecache memory settings recommendations.";
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
        return format(
                "Print heuristic memory setting recommendations for the Neo4j JVM heap and pagecache. The " +
                "heuristic is based on the total memory of the system the command is running on, or on the amount of " +
                "memory specified with the --memory argument. The heuristic assumes that the system is dedicated to " +
                "running Neo4j. If this is not the case, then use the --memory argument to specify how much memory " +
                "can be expected to be dedicated to Neo4j.%n" +
                "%n" +
                "The output is formatted such that it can be copy-posted into the neo4j.conf file." );
    }

    @Nonnull
    @Override
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new MemoryRecommendationsCommand( homeDir, configDir, outsideWorld );
    }
}
