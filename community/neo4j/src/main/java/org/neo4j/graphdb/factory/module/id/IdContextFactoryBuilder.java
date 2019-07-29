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
package org.neo4j.graphdb.factory.module.id;

import java.util.function.Function;

import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

public class IdContextFactoryBuilder
{
    private FileSystemAbstraction fileSystemAbstraction;
    private JobScheduler jobScheduler;
    private Function<DatabaseId,IdGeneratorFactory> idGeneratorFactoryProvider;
    private Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper;

    private IdContextFactoryBuilder()
    {
    }

    public static IdContextFactoryBuilder of( JobScheduler jobScheduler )
    {
        IdContextFactoryBuilder builder = new IdContextFactoryBuilder();
        builder.jobScheduler = jobScheduler;
        return builder;
    }

    public static IdContextFactoryBuilder of( FileSystemAbstraction fileSystemAbstraction, JobScheduler jobScheduler )
    {
        IdContextFactoryBuilder builder = new IdContextFactoryBuilder();
        builder.fileSystemAbstraction = fileSystemAbstraction;
        builder.jobScheduler = jobScheduler;
        return builder;
    }

    public IdContextFactoryBuilder withIdGenerationFactoryProvider( Function<DatabaseId,IdGeneratorFactory> idGeneratorFactoryProvider )
    {
        this.idGeneratorFactoryProvider = idGeneratorFactoryProvider;
        return this;
    }

    public IdContextFactoryBuilder withFactoryWrapper( Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper )
    {
        this.factoryWrapper = factoryWrapper;
        return this;
    }

    public IdContextFactory build()
    {
        if ( idGeneratorFactoryProvider == null )
        {
            requireNonNull( fileSystemAbstraction, "File system is required to build id generator factory." );
            // Note on the RecoveryCleanupWorkCollector: this is just using the immediate() because we aren't
            // expecting any cleanup to be performed on main startup (this is after recovery).
            idGeneratorFactoryProvider = databaseId -> new DefaultIdGeneratorFactory( fileSystemAbstraction, immediate() );
        }
        if ( factoryWrapper == null )
        {
            factoryWrapper = identity();
        }
        return new IdContextFactory( jobScheduler, idGeneratorFactoryProvider, factoryWrapper );
    }
}
