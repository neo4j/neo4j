/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.sampling;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.logging.Logging;

public class OnlineIndexSamplingJobFactory implements IndexSamplingJobFactory
{
    private final IndexStoreView storeView;
    private final Logging logging;
    private final TokenNameLookup nameLookup;

    public OnlineIndexSamplingJobFactory( IndexStoreView storeView, TokenNameLookup nameLookup, Logging logging )
    {
        this.storeView = storeView;
        this.logging = logging;
        this.nameLookup = nameLookup;
    }

    @Override
    public IndexSamplingJob create( IndexProxy indexProxy )
    {
        final String indexUserDescription = indexProxy.getDescriptor().userDescription( nameLookup );
        return new OnlineIndexSamplingJob( indexProxy, storeView, indexUserDescription, logging );
    }
}
