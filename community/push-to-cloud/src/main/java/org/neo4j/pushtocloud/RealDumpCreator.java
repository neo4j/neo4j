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

import picocli.CommandLine;

import java.io.File;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.DumpCommandProvider;

import static org.neo4j.internal.helpers.collection.Iterators.array;

class RealDumpCreator implements PushToCloudCommand.DumpCreator
{
    private ExecutionContext ctx;

    RealDumpCreator( ExecutionContext ctx )
    {
        this.ctx = ctx;
    }

    @Override
    public Path dumpDatabase( String database, Path targetDumpFile ) throws CommandFailedException
    {
        String[] args = array(
                "--database", database,
                "--to", targetDumpFile.toString() );
        new CommandLine( new DumpCommandProvider().createCommand( ctx ) ).execute( args );
        ctx.out().printf( "Dumped contents of database '%s' into '%s'%n", database, targetDumpFile );
        return targetDumpFile;
    }
}
