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
package org.neo4j.consistency.store;

import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.token.TokenHolders;

public class DirectStoreAccess
{
    private final NeoStores nativeStores;
    private final LabelScanStore labelScanStore;
    private final RelationshipTypeScanStore relationshipTypeScanstore;
    private final IndexProviderMap indexes;
    private final TokenHolders tokenHolders;
    private final IndexStatisticsStore indexStatisticsStore;
    private final IdGeneratorFactory idGeneratorFactory;

    public DirectStoreAccess( NeoStores nativeStores, LabelScanStore labelScanStore,
            RelationshipTypeScanStore relationshipTypeScanstore, IndexProviderMap indexes, TokenHolders tokenHolders,
            IndexStatisticsStore indexStatisticsStore, IdGeneratorFactory idGeneratorFactory )
    {
        this.nativeStores = nativeStores;
        this.labelScanStore = labelScanStore;
        this.relationshipTypeScanstore = relationshipTypeScanstore;
        this.indexes = indexes;
        this.tokenHolders = tokenHolders;
        this.indexStatisticsStore = indexStatisticsStore;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    public NeoStores nativeStores()
    {
        return nativeStores;
    }

    public LabelScanStore labelScanStore()
    {
        return labelScanStore;
    }

    public RelationshipTypeScanStore relationshipTypeScanStore()
    {
        return relationshipTypeScanstore;
    }

    public IndexProviderMap indexes()
    {
        return indexes;
    }

    public TokenHolders tokenHolders()
    {
        return tokenHolders;
    }

    public IndexStatisticsStore indexStatisticsStore()
    {
        return indexStatisticsStore;
    }

    public IdGeneratorFactory idGeneratorFactory()
    {
        return idGeneratorFactory;
    }
}
