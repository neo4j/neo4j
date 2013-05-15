/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.junit.Ignore;
import org.neo4j.kernel.api.index.IndexProviderCompatibilityTestSuite;
import org.neo4j.kernel.configuration.Config;

@Ignore("2013-05-15 The Lucene implementation does not support unique indexes yet.")
public class LuceneSchemaIndexProviderTest extends IndexProviderCompatibilityTestSuite
{
    @Override
    protected LuceneSchemaIndexProvider createIndexProvider()
    {
        return new LuceneSchemaIndexProvider( new DirectoryFactory.InMemoryDirectoryFactory(), new Config() );
    }
}

