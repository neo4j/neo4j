/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.index.InternalIndexState;

public interface FulltextProvider extends AutoCloseable
{
    FulltextProvider NULL_PROVIDER = new FulltextProvider()
    {
        @Override
        public void registerTransactionEventHandler() throws IOException
        {
            throw noProvider();
        }

        @Override
        public void awaitPopulation()
        {
            throw noProvider();
        }

        @Override
        public void openIndex( String identifier, FulltextIndexType type ) throws IOException
        {
            throw noProvider();
        }

        @Override
        public void createIndex( String identifier, FulltextIndexType type, List<String> properties ) throws IOException
        {
            throw noProvider();
        }

        @Override
        public ReadOnlyFulltext getReader( String identifier, FulltextIndexType type ) throws IOException
        {
            throw noProvider();
        }

        @Override
        public Set<String> getProperties( String identifier, FulltextIndexType type )
        {
            throw noProvider();
        }

        @Override
        public InternalIndexState getState( String identifier, FulltextIndexType type )
        {
            throw noProvider();
        }

        @Override
        public void changeIndexedProperties( String identifier, FulltextIndexType type, List<String> propertyKeys )
                throws IOException, InvalidArgumentsException
        {
            throw noProvider();
        }

        @Override
        public void close() throws Exception
        {
            throw noProvider();
        }

        private RuntimeException noProvider()
        {
            return new UnsupportedOperationException(
                    "There is no fulltext provider for this database. Make sure that the feature you are tyring to use is enabled" );
        }
    };
    String LUCENE_FULLTEXT_ADDON_PREFIX = "__lucene__fulltext__addon__";
    String FIELD_ENTITY_ID = LUCENE_FULLTEXT_ADDON_PREFIX + "internal__id__";

    void registerTransactionEventHandler() throws IOException;

    /**
     * Wait for the asynchronous background population, if one is on-going, to complete.
     * <p>
     * Such population, where the entire store is scanned for data to write to the index, will be started if the index
     * needs to recover after an unclean shut-down, or a configuration change.
     *
     * @throws RuntimeException If it was not possible to wait for the population to finish, for some reason.
     */
    void awaitPopulation();

    void openIndex( String identifier, FulltextIndexType type ) throws IOException;

    void createIndex( String identifier, FulltextIndexType type, List<String> properties ) throws IOException;

    /**
     * Returns a reader for the specified index.
     *
     * @param identifier Identifier for the index.
     * @param type Type of the index.
     * @return A {@link ReadOnlyFulltext} for the index, or null if no such index is found.
     * @throws IOException
     */
    ReadOnlyFulltext getReader( String identifier, FulltextIndexType type ) throws IOException;

    Set<String> getProperties( String identifier, FulltextIndexType type );

    InternalIndexState getState( String identifier, FulltextIndexType type );

    void changeIndexedProperties( String identifier, FulltextIndexType type, List<String> propertyKeys ) throws IOException, InvalidArgumentsException;
}
