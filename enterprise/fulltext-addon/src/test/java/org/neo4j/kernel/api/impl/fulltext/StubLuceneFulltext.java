/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import static java.util.Collections.singletonList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexType.NODES;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_NODES;

public class StubLuceneFulltext extends LuceneFulltext
{
    StubLuceneFulltext()
    {
        super( null, null, singletonList( "props" ), new StandardAnalyzer(), BLOOM_NODES, NODES );
    }
}
