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
package org.neo4j.causalclustering.discovery.data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.causalclustering.identity.MemberId;

public final class RefCounted<T>
{
    private final T value;
    private final Set<MemberId> references;

    public RefCounted( T value )
    {
        this( value, Collections.emptySet() );
    }

    public RefCounted( T value, MemberId holder )
    {
        this( value, Collections.singleton( holder ) );
    }

    public RefCounted( T value, Set<MemberId> references )
    {
        this.value = value;
        this.references = references;
    }

    public T value()
    {
        return value;
    }

    public <V> RefCounted<V> map( Function<T, V> mapper )
    {
        return new RefCounted<>( mapper.apply( value ), references );
    }

    public Set<MemberId> references()
    {
        return references;
    }

    public boolean safeToRemove()
    {
        return references.isEmpty();
    }

    public RefCounted<T> hold( MemberId instance )
    {
        HashSet<MemberId> newRefs = new HashSet<>( references );
        newRefs.add( instance );
        return new RefCounted<>( value, newRefs );
    }

    public RefCounted<T> release( MemberId instance )
    {
        HashSet<MemberId> newRefs = new HashSet<>( references );
        newRefs.remove( instance );
        return new RefCounted<>( value, newRefs );
    }

}
