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
package org.neo4j.kernel.api.impl.schema;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.GenericKey;

public class GenericStringLengthIndexValidationIT extends StringLengthIndexValidationIT
{
    @Override
    protected int getSingleKeySizeLimit()
    {
        int overhead = GenericKey.ENTITY_ID_SIZE + GenericKey.TYPE_ID_SIZE + GenericKey.SIZE_STRING_LENGTH;
        return TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE ) - overhead;
    }

    @Override
    protected GraphDatabaseSettings.SchemaIndex getSchemaIndex()
    {
        return GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
    }

    @Override
    protected String expectedPopulationFailureMessage()
    {
        return "Index key-value size it to large. Please see index documentation for limitations.";
    }
}
