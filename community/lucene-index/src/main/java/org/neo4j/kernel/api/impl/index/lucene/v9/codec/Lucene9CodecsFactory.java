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
package org.neo4j.kernel.api.impl.index.lucene.v9.codec;

import java.util.concurrent.ExecutorService;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodecsFactory;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;

public class Lucene9CodecsFactory implements LuceneCodecsFactory {
    public static final LuceneCodecsFactory INSTANCE = new Lucene9CodecsFactory();

    @Override
    public LuceneCodec codecFor(VectorIndexConfig config, int numMergeWorkers, ExecutorService mergeExec) {
        // we don't support intra-parallel workers with Lucene 9
        return new VectorCodecV2(config);
    }
}
