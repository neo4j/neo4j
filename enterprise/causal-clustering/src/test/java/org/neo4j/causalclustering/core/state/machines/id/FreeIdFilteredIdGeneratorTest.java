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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import java.util.function.BooleanSupplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class FreeIdFilteredIdGeneratorTest
{

    private IdGenerator idGenerator = mock( IdGenerator.class );

    @Test
    public void freeIdIfConditionSatisfied() throws Exception
    {
        FreeIdFilteredIdGenerator generator = createFilteredIdGenerator( idGenerator, () -> true );
        generator.freeId( 1 );

        verify( idGenerator ).freeId( 1 );
    }

    @Test
    public void skipFreeIdIfConditionIsNotSatisfied()
    {
        FreeIdFilteredIdGenerator generator = createFilteredIdGenerator( idGenerator, () -> false );
        generator.freeId( 1 );

        verifyZeroInteractions( idGenerator );
    }

    @Test
    public void freeIdOnlyWhenConditionSatisfied()
    {
        MutableBoolean condition = new MutableBoolean();
        FreeIdFilteredIdGenerator generator = createFilteredIdGenerator( idGenerator, condition::booleanValue );
        generator.freeId( 1 );
        condition.setTrue();
        generator.freeId( 2 );

        verify( idGenerator ).freeId( 2 );
    }

    private FreeIdFilteredIdGenerator createFilteredIdGenerator( IdGenerator idGenerator,
            BooleanSupplier booleanSupplier )
    {
        return new FreeIdFilteredIdGenerator( idGenerator, booleanSupplier );
    }
}
