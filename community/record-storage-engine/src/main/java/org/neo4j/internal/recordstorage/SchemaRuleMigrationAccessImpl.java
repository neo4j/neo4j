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
package org.neo4j.internal.recordstorage;

import java.io.IOException;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;

public class SchemaRuleMigrationAccessImpl implements SchemaRuleMigrationAccess
{
    private final NeoStores neoStores;
    private final SchemaStorage schemaStorage;

    SchemaRuleMigrationAccessImpl( NeoStores neoStores, SchemaStorage schemaStorage )
    {
        this.neoStores = neoStores;
        this.schemaStorage = schemaStorage;
    }

    @Override
    public Iterable<SchemaRule> getAll()
    {
        return schemaStorage.getAll();
    }

    @Override
    public void writeSchemaRule( SchemaRule rule ) throws KernelException
    {
        schemaStorage.writeSchemaRule( rule );
    }

    @Override
    public void close() throws IOException
    {
        neoStores.flush( IOLimiter.UNLIMITED );
        neoStores.close();
    }
}
