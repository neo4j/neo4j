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
package org.neo4j.kernel.api.impl.schema;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.LayoutTestUtil;
import org.neo4j.test.rule.RandomRule;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;

public class GenericStringLengthIndexValidationIT extends StringLengthIndexValidationIT
{
    @Override
    protected int getSingleKeySizeLimit()
    {
        return TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE );
    }

    @Override
    protected String getString( RandomRule random, int keySize )
    {
        return LayoutTestUtil.generateStringResultingInSizeForIndexProvider( keySize, NATIVE_BTREE10 );
    }

    @Override
    protected GraphDatabaseSettings.SchemaIndex getSchemaIndex()
    {
        return NATIVE_BTREE10;
    }

    @Override
    protected String expectedPopulationFailureMessage()
    {
        return "Index key-value size it to large. Please see index documentation for limitations.";
    }

    @Override
    protected String expectedPopulationFailureCauseMessage( long nodeId )
    {
        return format( "Failed while trying to write to index, targetIndex=Index( 1, 'index_71616483', GENERAL BTREE, :LABEL_ONE(largeString), %s ), " +
                "nodeId=%d", NATIVE_BTREE10.providerName(), nodeId );
    }
}
