/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.function.BooleanSupplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;

/**
 * Id generator that will perform filtering of ids to free using supplied condition.
 * Id will be freed only if condition is true, otherwise it will be ignored.
 */
public class FreeIdFilteredIdGenerator extends IdGenerator.Delegate
{
    private final BooleanSupplier freeIdCondition;

    FreeIdFilteredIdGenerator( IdGenerator delegate, BooleanSupplier freeIdCondition )
    {
        super( delegate );
        this.freeIdCondition = freeIdCondition;
    }

    @Override
    public void freeId( long id )
    {
        if ( freeIdCondition.getAsBoolean() )
        {
            super.freeId( id );
        }
    }
}
