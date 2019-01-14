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
package org.neo4j.unsafe.impl.batchimport.store;

import java.util.function.Function;
import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Exists to allow {@link IdSequence} with specific behaviour relevant to import to be injected into
 * {@link CommonAbstractStore#prepareForCommit(AbstractBaseRecord, IdSequence)}.
 */
public interface PrepareIdSequence extends Function<IdSequence,LongFunction<IdSequence>>
{
    static PrepareIdSequence of( boolean doubleUnits )
    {
        return doubleUnits ? new SecondaryUnitPrepareIdSequence() : new StorePrepareIdSequence();
    }
}
