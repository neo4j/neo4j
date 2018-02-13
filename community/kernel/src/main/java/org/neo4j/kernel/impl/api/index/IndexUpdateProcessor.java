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

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.LabelSchemaSupplier;

public interface IndexUpdateProcessor<INDEX_KEY extends LabelSchemaSupplier>
{
    void accept( IndexEntryUpdate<INDEX_KEY> update ) throws IOException, IndexEntryConflictException;

    static <INDEX_KEY extends LabelSchemaSupplier> void applyIndexUpdatesByMode( Iterable<IndexEntryUpdate<INDEX_KEY>> updates,
            IndexUpdateProcessor<INDEX_KEY> target )
            throws IOException, IndexEntryConflictException
    {
        // deletes
        for ( IndexEntryUpdate<INDEX_KEY> indexUpdate : updates )
        {
            if ( indexUpdate.updateMode() == UpdateMode.REMOVED )
            {
                target.accept( indexUpdate );
            }
            else if ( indexUpdate.updateMode() == UpdateMode.CHANGED )
            {
                target.accept( removalOfChange( indexUpdate ) );
            }
        }
        // adds
        for ( IndexEntryUpdate<INDEX_KEY> indexUpdate : updates )
        {
            if ( indexUpdate.updateMode() == UpdateMode.ADDED )
            {
                target.accept( indexUpdate );
            }
            else if ( indexUpdate.updateMode() == UpdateMode.CHANGED )
            {
                target.accept( additionOfChange( indexUpdate ) );
            }
        }
    }

    static <INDEX_KEY extends LabelSchemaSupplier> IndexEntryUpdate<INDEX_KEY> additionOfChange( IndexEntryUpdate<INDEX_KEY> indexUpdate )
    {
        return IndexEntryUpdate.add( indexUpdate.getEntityId(), indexUpdate.indexKey(), indexUpdate.values() );
    }

    static <INDEX_KEY extends LabelSchemaSupplier> IndexEntryUpdate<INDEX_KEY> removalOfChange( IndexEntryUpdate<INDEX_KEY> indexUpdate )
    {
        return IndexEntryUpdate.remove( indexUpdate.getEntityId(), indexUpdate.indexKey(), indexUpdate.beforeValues() );
    }
}
