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
package org.neo4j.index.impl.lucene;

import java.util.Collection;
import java.util.Iterator;

class ConstantScoreIterator extends AbstractLegacyIndexHits
{
    private final Iterator<EntityId> items;
    private final int size;
    private final float score;

    ConstantScoreIterator( Collection<EntityId> items, float score )
    {
        this.items = items.iterator();
        this.score = score;
        this.size = items.size();
    }

    @Override
    public float currentScore()
    {
        return this.score;
    }

    @Override
    public int size()
    {
        return this.size;
    }

    @Override
    protected boolean fetchNext()
    {
        return items.hasNext() && next( items.next().id() );
    }
}
