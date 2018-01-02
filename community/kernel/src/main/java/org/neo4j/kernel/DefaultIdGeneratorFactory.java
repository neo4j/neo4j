/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
// TODO 3.0: Move to org.neo4j.kernel.impl.store.id package
@Deprecated
public class DefaultIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType, IdGenerator> generators = new HashMap<>();
    private final FileSystemAbstraction fs;
    private final IdTypeConfigurationProvider idTypeConfigurationProvider;

    public DefaultIdGeneratorFactory( FileSystemAbstraction fs )
    {
        this( fs, new CommunityIdTypeConfigurationProvider() );
    }

    public DefaultIdGeneratorFactory( FileSystemAbstraction fs, IdTypeConfigurationProvider idTypeConfigurationProvider)
    {
        this.fs = fs;
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, long highId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return open( filename, idTypeConfiguration.getGrabSize(), idType, idTypeConfiguration.allowAggressiveReuse(), highId );
    }

    public IdGenerator open( File fileName, int grabSize, IdType idType, long highId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return open( fileName, grabSize, idType, idTypeConfiguration.allowAggressiveReuse(), highId );
    }

    private IdGenerator open( File fileName, int grabSize, IdType idType, boolean aggressiveReuse, long highId )
    {
        long maxValue = idType.getMaxValue();
        IdGenerator generator = new IdGeneratorImpl( fs, fileName, grabSize, maxValue,
                aggressiveReuse, highId );
        generators.put( idType, generator );
        return generator;
    }

    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }

    public void create( File fileName, long highId, boolean throwIfFileExists )
    {
        IdGeneratorImpl.createGenerator( fs, fileName, highId, throwIfFileExists );
    }
}
