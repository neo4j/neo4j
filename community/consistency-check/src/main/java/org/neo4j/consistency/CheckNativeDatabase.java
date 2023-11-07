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
package org.neo4j.consistency;

import java.io.PrintStream;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.archive.CheckDatabase;
import org.neo4j.dbms.archive.CheckDatabase.Source.DataTxnSource;
import org.neo4j.io.IOUtils.AutoCloseables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.storageengine.api.StorageEngineFactory;

@ServiceProvider
public class CheckNativeDatabase implements CheckDatabase {
    @Override
    public String name() {
        return "database";
    }

    @Override
    public boolean containsPotentiallyCheckableDatabase(
            FileSystemAbstraction fs, Source source, NormalizedDatabaseName database) {
        return source instanceof final DataTxnSource dataTxnSource
                && StorageEngineFactory.selectStorageEngine(fs, targetLayoutFrom(fs, dataTxnSource, database, null))
                        .isPresent();
    }

    @Override
    public DatabaseLayout targetLayoutFrom(
            FileSystemAbstraction fs,
            Source source,
            NormalizedDatabaseName database,
            AutoCloseables<?> autoCloseables) {
        final var dataTxnSource = Source.expected(DataTxnSource.class, source);
        return dataTxnSource.layout.databaseLayout(database.name());
    }

    @Override
    public void tryExtract(
            FileSystemAbstraction fs,
            Config config,
            DatabaseLayout targetLayout,
            Source source,
            NormalizedDatabaseName database,
            PrintStream out,
            boolean force) {
        Source.expected(DataTxnSource.class, source);
        // no-op
    }
}
