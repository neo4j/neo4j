/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

public class IdModuleBuilder
{
    private IdReuseEligibility idReuseEligibility = IdReuseEligibility.ALWAYS;
    private FileSystemAbstraction fileSystemAbstraction;
    private JobScheduler jobScheduler;
    private IdTypeConfigurationProvider idTypeConfigurationProvider;
    private Function<String,IdGeneratorFactory> idGeneratorFactoryProvider;

    public static IdModuleBuilder of( IdTypeConfigurationProvider configurationProvider )
    {
        IdModuleBuilder builder = new IdModuleBuilder();
        builder.idTypeConfigurationProvider = configurationProvider;
        return builder;
    }

    public static IdModuleBuilder of( FileSystemAbstraction fileSystemAbstraction, JobScheduler jobScheduler )
    {
        IdModuleBuilder builder = new IdModuleBuilder();
        builder.fileSystemAbstraction = fileSystemAbstraction;
        builder.jobScheduler = jobScheduler;
        return builder;
    }

    public IdModuleBuilder withFileSystem( FileSystemAbstraction fileSystem )
    {
        this.fileSystemAbstraction = fileSystem;
        return this;
    }

    public IdModuleBuilder withJobScheduler( JobScheduler jobScheduler )
    {
        this.jobScheduler = jobScheduler;
        return this;
    }

    public IdModuleBuilder withIdReuseEligibility( IdReuseEligibility eligibleForIdReuse )
    {
        this.idReuseEligibility = eligibleForIdReuse;
        return this;
    }

    public IdModuleBuilder withIdGenerationFactoryProvider( Function<String,IdGeneratorFactory> idGeneratorFactoryProvider )
    {
        this.idGeneratorFactoryProvider = idGeneratorFactoryProvider;
        return this;
    }

    public IdModule build()
    {
        if ( idGeneratorFactoryProvider == null )
        {
            idGeneratorFactoryProvider = databaseName -> new DefaultIdGeneratorFactory( fileSystemAbstraction, idTypeConfigurationProvider );
        }
        if ( idTypeConfigurationProvider == null )
        {
            idTypeConfigurationProvider = new CommunityIdTypeConfigurationProvider();
        }
        // todo pass id buffering
        return new IdModule( jobScheduler, idGeneratorFactoryProvider, idTypeConfigurationProvider, idReuseEligibility, true );
    }
}
