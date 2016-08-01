/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.core.state.machines.id;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;

public class ReplicatedIdGeneratorFactory extends LifecycleAdapter implements IdGeneratorFactory
{
    private final Map<IdType, SwitchableRaftIdGenerator> generators = new HashMap<>();
    private final FileSystemAbstraction fs;
    private final ReplicatedIdRangeAcquirer idRangeAcquirer;
    private final LogProvider logProvider;
    private IdTypeConfigurationProvider idTypeConfigurationProvider;
    private boolean replicatedMode = false;

    public ReplicatedIdGeneratorFactory( FileSystemAbstraction fs, ReplicatedIdRangeAcquirer idRangeAcquirer,
                                   LogProvider logProvider, IdTypeConfigurationProvider idTypeConfigurationProvider )
    {
        this.fs = fs;
        this.idRangeAcquirer = idRangeAcquirer;
        this.logProvider = logProvider;
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, long highId, long maxId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return openGenerator( filename, idTypeConfiguration.getGrabSize(), idType, highId, maxId,
                idTypeConfiguration.allowAggressiveReuse() );
    }

    @Override
    public IdGenerator open( File fileName, int grabSize, IdType idType, long highId, long maxId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        boolean aggressiveReuse = idTypeConfiguration.allowAggressiveReuse();
        return openGenerator( fileName, grabSize, idType, highId, maxId, aggressiveReuse );
    }

    private IdGenerator openGenerator( File fileName, int grabSize, IdType idType, long highId, long maxId,
            boolean aggressiveReuse )
    {
        SwitchableRaftIdGenerator previous = generators.remove( idType );
        if ( previous != null )
        {
            previous.close();
        }

        IdGenerator initialIdGenerator = new IdGeneratorImpl( fs, fileName, grabSize, maxId, aggressiveReuse, highId );
        SwitchableRaftIdGenerator switchableIdGenerator =
                new SwitchableRaftIdGenerator( initialIdGenerator, idType, idRangeAcquirer, logProvider );
        if ( replicatedMode )
        {
            switchableIdGenerator.switchToRaft();
        }

        generators.put( idType, switchableIdGenerator );
        return switchableIdGenerator;
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }

    @Override
    public void create( File fileName, long highId, boolean throwIfFileExists )
    {
        IdGeneratorImpl.createGenerator( fs, fileName, highId, throwIfFileExists );
    }

    @Override
    public void start() throws Throwable
    {
        replicatedMode = true;
        for ( SwitchableRaftIdGenerator switchableRaftIdGenerator : generators.values() )
        {
            switchableRaftIdGenerator.switchToRaft();
        }
    }
}
