/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;

public interface FulltextProvider extends AutoCloseable
{
    FulltextProvider NULL_PROVIDER = new FulltextProvider()
    {

        @Override
        public void awaitPopulation()
        {
            throw noProvider();
        }

        @Override
        public void openIndex( String identifier, FulltextIndexType type )
        {
            throw noProvider();
        }

        @Override
        public void createIndex( String identifier, FulltextIndexType type, List<String> properties )
        {
            throw noProvider();
        }

        @Override
        public ReadOnlyFulltext getReader( String identifier, FulltextIndexType type )
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
        {
            throw noProvider();
        }

        @Override
        public void registerFileListing( NeoStoreFileListing fileListing )
        {
            throw noProvider();
        }

        @Override
        public void awaitFlip()
        {
            throw noProvider();
        }

        @Override
        public void close()
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

    /**
     * Wait for the asynchronous background population, if one is on-going, to complete.
     * <p>
     * Such population, where the entire store is scanned for data to write to the index, will be started if the index
     * needs to recover after an unclean shut-down, or a configuration change.
     *
     * @throws UncheckedIOException If it was not possible to wait for the population to finish, for some reason.
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

    void registerFileListing( NeoStoreFileListing fileListing );

    void awaitFlip();
}
