/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class DefaultIdGeneratorFactory
    implements IdGeneratorFactory
{
    private final Map<IdType, IdGenerator> generators = new HashMap<IdType, IdGenerator>();

    public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType, long highId )
    {
        IdGenerator generator = new IdGeneratorImpl( fs, fileName, grabSize, idType.getMaxValue(),
                idType.allowAggressiveReuse(), highId );
        generators.put( idType, generator );
        return generator;
    }

    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }

    public void create( FileSystemAbstraction fs, File fileName, long highId )
    {
        IdGeneratorImpl.createGenerator( fs, fileName, highId );
    }
}
