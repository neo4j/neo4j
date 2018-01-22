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
package org.neo4j.collection.primitive;

public class CombinePrimitiveLongSet implements PrimitiveLongSet
{
    private final PrimitiveLongSet setA;
    private final PrimitiveLongSet setB;

    public CombinePrimitiveLongSet( PrimitiveLongSet setA, PrimitiveLongSet setB )
    {
        this.setA = setA;
        this.setB = setB;
    }

    @Override
    public boolean add( long value )
    {
        throw new UnsupportedOperationException( "Modification is not supported in " + getClass() );
    }

    @Override
    public boolean addAll( PrimitiveLongIterator values )
    {
        throw new UnsupportedOperationException( "Modification is not supported in " + getClass() );
    }

    @Override
    public boolean contains( long value )
    {
        return setA.contains( value ) || setB.contains( value );
    }

    @Override
    public boolean remove( long value )
    {
        throw new UnsupportedOperationException( "Modification is not supported in " + getClass() );
    }

    @Override
    public boolean test( long value )
    {
        return setA.test( value ) || setB.test( value );
    }

    @Override
    public <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
    {

    }

    @Override
    public boolean isEmpty()
    {
        return setA.isEmpty() && setB.isEmpty();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException( "Modification is not supported in " + getClass() );
    }

    @Override
    public int size()
    {
        return setA.size() + setB.size();
    }

    @Override
    public void close()
    {

    }

    @Override
    public PrimitiveLongIterator iterator()
    {
        return PrimitiveLongCollections.concat( setA.iterator(), setB.iterator() );
    }
}
