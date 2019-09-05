/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.dbms.DumpCommandProvider;

import static org.neo4j.helpers.collection.Iterators.array;

class RealDumpCreator implements PushToCloudCommand.DumpCreator
{
    private final Path homeDir;
    private final Path configDir;
    private final OutsideWorld outsideWorld;

    RealDumpCreator( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    @Override
    public void dumpDatabase( String database, Path targetDumpFile ) throws CommandFailed, IncorrectUsage
    {
        String[] args = array(
                "--database", database,
                "--to", targetDumpFile.toString() );
        new DumpCommandProvider().create( homeDir, configDir, outsideWorld ).execute( args );
        outsideWorld.outStream().printf( "Dumped contents of database '%s' into '%s'%n", database, targetDumpFile );
    }
}
