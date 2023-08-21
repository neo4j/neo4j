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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.kernel.api.impl.index.WritableDatabaseIndex;

class WritableFulltextDatabaseIndex extends WritableDatabaseIndex<FulltextIndex, FulltextIndexReader> {
    private final IndexUpdateSink indexUpdateSink;

    WritableFulltextDatabaseIndex(
            IndexUpdateSink indexUpdateSink,
            FulltextIndex fulltextIndex,
            DatabaseReadOnlyChecker readOnlyChecker,
            boolean permanentlyReadOnly) {
        super(fulltextIndex, readOnlyChecker, permanentlyReadOnly);
        this.indexUpdateSink = indexUpdateSink;
    }

    @Override
    public String toString() {
        return luceneIndex.toString();
    }

    @Override
    protected void commitLockedFlush() throws IOException {
        indexUpdateSink.awaitUpdateApplication();
        super.commitLockedFlush();
    }

    @Override
    protected void commitLockedClose() throws IOException {
        indexUpdateSink.awaitUpdateApplication();
        super.commitLockedClose();
    }
}
