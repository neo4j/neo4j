/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import org.neo4j.index.Hit;

/**
 * Straight-forward implement of {@link Hit} where focus is on zero garbage. This instances can be used
 * for multiple hits. Key/value instances are provided, its values overwritten for every hit and
 * merely be exposed in {@link #key()} and {@link #value()}, where caller must adhere to not keeping
 * references to those instances after reading them.
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
public class MutableHit<KEY,VALUE> implements Hit<KEY,VALUE>
{
    private final KEY key;
    private final VALUE value;

    /**
     * Constructs a new {@link MutableHit} where the provided {@code key} and {@code value} are single
     * instances to be re-used and overwritten for every hit in a result set.
     *
     * @param key KEY instance to reuse for every hit.
     * @param value VALUE instance to reuse for every hit.
     */
    public MutableHit( KEY key, VALUE value )
    {
        this.key = key;
        this.value = value;
    }

    /**
     * @return key instance containing current key. This instance will have its value overwritten for next
     * hit and so no reference to the returned instances must be kept after reading it.
     */
    @Override
    public KEY key()
    {
        return key;
    }

    /**
     * @return value instance containing current value. This instance will have its value overwritten for next
     * hit and so no reference to the returned instances must be kept after reading it.
     */
    @Override
    public VALUE value()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return "[key:" + key + ", value:" + value + "]";
    }
}
