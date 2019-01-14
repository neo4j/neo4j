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
package org.neo4j.kernel.spi.explicitindex;

import java.io.Closeable;
import java.util.Map;

import org.neo4j.kernel.api.ExplicitIndex;

/**
 * Provides access to {@link ExplicitIndex indexes}. Holds transaction state for a specific provider in a transaction.
 * This interface is implemented by the index provider.
 */
public interface ExplicitIndexProviderTransaction extends Closeable
{
    ExplicitIndex nodeIndex( String indexName, Map<String, String> configuration );

    ExplicitIndex relationshipIndex( String indexName, Map<String, String> configuration );

    @Override
    void close();
}
