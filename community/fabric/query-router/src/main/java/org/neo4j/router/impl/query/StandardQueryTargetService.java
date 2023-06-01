/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.router.impl.query;

import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.router.query.DatabaseReferenceResolver;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryTargetParser;

public class StandardQueryTargetService extends AbstractQueryTargetService {

    private final QueryTargetParser queryTargetParser;
    private final DatabaseReferenceResolver databaseReferenceResolver;

    public StandardQueryTargetService(
            DatabaseReference sessionDatabase,
            QueryTargetParser queryTargetParser,
            DatabaseReferenceResolver databaseReferenceResolver) {
        super(sessionDatabase);
        this.queryTargetParser = queryTargetParser;
        this.databaseReferenceResolver = databaseReferenceResolver;
    }

    @Override
    public DatabaseReference determineTarget(Query query) {
        var parsedTarget = queryTargetParser
                .parseQueryTarget(query)
                .map(CatalogName::qualifiedNameString)
                .map(databaseReferenceResolver::resolve);
        return parsedTarget.orElse(sessionDatabase);
    }
}
