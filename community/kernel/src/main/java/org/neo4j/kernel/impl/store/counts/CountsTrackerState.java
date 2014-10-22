/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.register.Register;

interface CountsTrackerState extends Closeable
{
    long lastTxId();

    boolean hasChanges();

    long nodeCount( CountsKey.NodeKey nodeKey );

    long relationshipCount( CountsKey.RelationshipKey relationshipKey );

    long indexSizeCount( CountsKey.IndexSizeKey indexSizeKey );

    long incrementNodeCount( CountsKey.NodeKey nodeKey, long delta );

    long incrementRelationshipCount( CountsKey.RelationshipKey relationshipKey, long delta );

    long incrementIndexSizeCount( CountsKey.IndexSizeKey indexSizeKey, long delta );

    void replaceIndexSizeCount( CountsKey.IndexSizeKey indexSizeKey, long total );

    File storeFile();

    SortedKeyValueStore.Writer<CountsKey, Register.DoubleLongRegister> newWriter( File file, long lastCommittedTxId )
            throws IOException;

    void accept( KeyValueRecordVisitor<CountsKey, Register.DoubleLongRegister> visitor );
}
