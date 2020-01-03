/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class IdContextFactoryBuilder
{
    private IdReuseEligibility idReuseEligibility = IdReuseEligibility.ALWAYS;
    private FileSystemAbstraction fileSystemAbstraction;
    private JobScheduler jobScheduler;
    private Function<String,IdGeneratorFactory> idGeneratorFactoryProvider;
    private IdTypeConfigurationProvider idTypeConfigurationProvider;
    private Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper;

    private IdContextFactoryBuilder()
    {
    }

    public static IdContextFactoryBuilder of( IdTypeConfigurationProvider configurationProvider, JobScheduler jobScheduler )
    {
        IdContextFactoryBuilder builder = new IdContextFactoryBuilder();
        builder.idTypeConfigurationProvider = configurationProvider;
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

    public IdContextFactoryBuilder withFileSystem( FileSystemAbstraction fileSystem )
    {
        this.fileSystemAbstraction = fileSystem;
        return this;
    }

    public IdContextFactoryBuilder withIdReuseEligibility( IdReuseEligibility eligibleForIdReuse )
    {
        this.idReuseEligibility = eligibleForIdReuse;
        return this;
    }

    public IdContextFactoryBuilder withIdGenerationFactoryProvider( Function<String,IdGeneratorFactory> idGeneratorFactoryProvider )
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
            idGeneratorFactoryProvider = databaseName -> new DefaultIdGeneratorFactory( fileSystemAbstraction, idTypeConfigurationProvider );
        }
        if ( idTypeConfigurationProvider == null )
        {
            idTypeConfigurationProvider = new CommunityIdTypeConfigurationProvider();
        }
        if ( factoryWrapper == null )
        {
            factoryWrapper = identity();
        }
        return new IdContextFactory( jobScheduler, idGeneratorFactoryProvider, idTypeConfigurationProvider, idReuseEligibility, factoryWrapper );
    }
}
