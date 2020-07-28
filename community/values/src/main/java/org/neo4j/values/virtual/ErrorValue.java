/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.values.virtual;

import java.util.Comparator;

import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOf;

/**
 * The ErrorValue allow delaying errors in value creation until runtime, which is useful
 * if it turns out that the value is never used.
 */
public final class ErrorValue extends VirtualValue
{
    private static final long INVALID_ARGUMENT_EXCEPTION_SHALLOW_SIZE = shallowSizeOfInstance( InvalidArgumentException.class );
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( ErrorValue.class ) + INVALID_ARGUMENT_EXCEPTION_SHALLOW_SIZE;
    private final InvalidArgumentException e;

    ErrorValue( Exception e )
    {
        this.e = new InvalidArgumentException( e.getMessage() );
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        throw e;
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.ERROR;
    }

    @Override
    public int unsafeCompareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        throw e;
    }

    @Override
    public Comparison unsafeTernaryCompareTo( VirtualValue other, TernaryComparator<AnyValue> comparator )
    {
        throw e;
    }

    @Override
    protected int computeHashToMemoize()
    {
        throw e;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer )
    {
        throw e;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        throw e;
    }

    @Override
    public String getTypeName()
    {
        return "Error";
    }

    @Override
    public long estimatedHeapUsage()
    {
        return SHALLOW_SIZE + sizeOf( e.getMessage() ); // We ignore stacktrace for now, ideally we should get rid of this whole class
    }
}
