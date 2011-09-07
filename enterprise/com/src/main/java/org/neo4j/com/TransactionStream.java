/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.com;

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.PrefetchingIterator;

/**
 * Represents a stream of the data of one or more consecutive transactions.
 */
public class TransactionStream extends
        PrefetchingIterator<Triplet<String/*datasource*/, Long/*txid*/, TxExtractor>>
{
    public static final TransactionStream EMPTY = new TransactionStream( Collections.<Triplet<String, Long, TxExtractor>>emptyList().iterator() );
    private final String[] datasources;
    private final Iterator<Triplet<String, Long, TxExtractor>> iterator;

    public TransactionStream( Iterator<Triplet<String, Long, TxExtractor>> iterator, String... datasources )
    {
        this.iterator = iterator;
        this.datasources = datasources;
    }

    public String[] dataSourceNames()
    {
        return datasources.clone();
    }
    
    @Override
    protected Triplet<String, Long, TxExtractor> fetchNextOrNull()
    {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
