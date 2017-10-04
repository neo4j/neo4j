/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;

public class FreeIdFilteredIdGeneratorFactory implements IdGeneratorFactory
{
    private Map<IdType, IdGenerator> delegatedGenerator = new HashMap<>();
    private final IdGeneratorFactory delegate;
    private final BooleanSupplier freeIdCondition;

    public FreeIdFilteredIdGeneratorFactory( IdGeneratorFactory delegate, BooleanSupplier freeIdCondition )
    {
        this.delegate = delegate;
        this.freeIdCondition = freeIdCondition;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, Supplier<Long> highId, long maxId )
    {
        FreeIdFilteredIdGenerator freeIdFilteredIdGenerator =
                new FreeIdFilteredIdGenerator( delegate.open( filename, idType, highId, maxId ), freeIdCondition );
        delegatedGenerator.put( idType, freeIdFilteredIdGenerator );
        return freeIdFilteredIdGenerator;
    }

    @Override
    public IdGenerator open( File filename, int grabSize, IdType idType, Supplier<Long> highId, long maxId )
    {
        FreeIdFilteredIdGenerator freeIdFilteredIdGenerator =
                new FreeIdFilteredIdGenerator( delegate.open( filename, grabSize, idType, highId, maxId ),
                        freeIdCondition );
        delegatedGenerator.put( idType, freeIdFilteredIdGenerator );
        return freeIdFilteredIdGenerator;
    }

    @Override
    public void create( File filename, long highId, boolean throwIfFileExists )
    {
        delegate.create( filename, highId, throwIfFileExists );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return delegatedGenerator.get( idType );
    }
}
