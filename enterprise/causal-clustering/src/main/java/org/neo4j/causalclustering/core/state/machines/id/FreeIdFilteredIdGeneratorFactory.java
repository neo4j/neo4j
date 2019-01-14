/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.id;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

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
    public IdGenerator open( File filename, IdType idType, LongSupplier highId, long maxId )
    {
        FreeIdFilteredIdGenerator freeIdFilteredIdGenerator =
                new FreeIdFilteredIdGenerator( delegate.open( filename, idType, highId, maxId ), freeIdCondition );
        delegatedGenerator.put( idType, freeIdFilteredIdGenerator );
        return freeIdFilteredIdGenerator;
    }

    @Override
    public IdGenerator open( File filename, int grabSize, IdType idType, LongSupplier highId, long maxId )
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
