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
package org.neo4j.kernel.impl.api.index;

import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;

public interface IndexProviderMap extends Function<IndexProvider.Descriptor,IndexProvider>
{
    IndexProviderMap EMPTY = new IndexProviderMap()
    {
        @Override
        public IndexProvider apply( IndexProvider.Descriptor descriptor ) throws IndexProviderNotFoundException
        {
            return IndexProvider.NO_INDEX_PROVIDER;
        }

        @Override
        public IndexProvider<SchemaIndexDescriptor> getDefaultSchemaIndexProvider()
        {
            return IndexProvider.NO_INDEX_PROVIDER;
        }

        @Override
        public IndexProvider getProviderFor( IndexDescriptor descriptor )
        {
            return IndexProvider.NO_INDEX_PROVIDER;
        }

        @Override
        public void accept( Consumer<IndexProvider> visitor )
        {
            //Sure
        }
    };

    @Override
    IndexProvider apply( IndexProvider.Descriptor descriptor ) throws IndexProviderNotFoundException;

    IndexProvider<SchemaIndexDescriptor> getDefaultSchemaIndexProvider();

    IndexProvider getProviderFor( IndexDescriptor descriptor );

    void accept( Consumer<IndexProvider> visitor );
}
