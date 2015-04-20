/*
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
package org.neo4j.kernel.impl.store.standard;

/**
 * This is a wrapper around {@link org.neo4j.kernel.impl.nioneo.store.IdGenerator}, containing only those
 * components used by the stores. This is used while rebuilding the store layer, and may or may not remain once that
 * rebuild is complete.
 */
public interface StoreIdGenerator
{
    public long allocate();
    public void free( long id );

    long highestIdInUse();
    void setHighestIdInUse( long highId );

    /** Clear all internal state of this id generator, and mark all ids as in use up to and including the id here. */
    void rebuild( long highestIdInUse );
}
