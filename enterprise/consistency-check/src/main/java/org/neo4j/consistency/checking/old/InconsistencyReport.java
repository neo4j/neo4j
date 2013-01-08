/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.consistency.checking.old;

import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

@Deprecated
public interface InconsistencyReport
{
    // Inconsistency between two records
    <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred, InconsistencyType type );

    <R extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R> store, R record, R referred, InconsistencyType type ) ;

    // Internal inconsistency in a single record
    <R extends AbstractBaseRecord> boolean inconsistent( RecordStore<R> store, R record, InconsistencyType type );

}
