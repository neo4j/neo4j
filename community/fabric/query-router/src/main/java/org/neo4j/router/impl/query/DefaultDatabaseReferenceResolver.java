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

import java.util.function.Supplier;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.router.query.DatabaseReferenceResolver;

public class DefaultDatabaseReferenceResolver implements DatabaseReferenceResolver {
    private final DatabaseReferenceRepository repository;

    public DefaultDatabaseReferenceResolver(DatabaseReferenceRepository repository) {
        this.repository = repository;
    }

    @Override
    public DatabaseReference resolve(String name) {
        return resolve(new NormalizedDatabaseName(name));
    }

    @Override
    public DatabaseReference resolve(NormalizedDatabaseName name) {
        return repository.getByAlias(name).orElseThrow(databaseNotFound(name));
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(NormalizedDatabaseName databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Database " + databaseNameRaw.name() + " not found");
    }
}
