/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.router.impl.query;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.router.query.TargetService;

/**
 * A transaction-scoped caching target service that ensures
 * that name to database reference mapping is stable for queries
 * in one transaction.
 */
public class TransactionTargetService implements TargetService {

    private final Map<CatalogInfo, DatabaseReference> cache = new HashMap<>();
    private final TargetService delegate;

    public TransactionTargetService(TargetService delegate) {
        this.delegate = delegate;
    }

    @Override
    public DatabaseReference target(CatalogInfo catalogInfo) {
        return cache.computeIfAbsent(catalogInfo, delegate::target);
    }
}
