/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.pushtocloud;

import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;

public class PushToCloudCommandProvider extends AdminCommand.Provider
{
    public PushToCloudCommandProvider()
    {
        super( "push-to-cloud" );
    }

    @Override
    public Arguments allArguments()
    {
        return PushToCloudCommand.arguments;
    }

    @Override
    public String summary()
    {
        return "Push database to Neo4j cloud";
    }

    @Override
    public AdminCommandSection commandSection()
    {
        return AdminCommandSection.general();

    }

    @Override
    public String description()
    {
        return "Push your local database to a Neo4j Aura instance. The database must be shutdown in order to take a dump to upload. " +
                "The target location is your Neo4j Aura Bolt URI. You will be asked your Neo4j Cloud username and password during " +
                "the push-to-cloud operation.";
    }

    @Override
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new PushToCloudCommand( homeDir, configDir, outsideWorld, new HttpCopier( outsideWorld ),
                new RealDumpCreator( homeDir, configDir, outsideWorld ) );
    }
}
