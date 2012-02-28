/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexIterable;
import org.neo4j.graphdb.index.IndexProvider;

/**
 * @author ceefour
 * A {@link GraphDatabaseFactory} that can have its dependencies configured,
 * to make it easy to be used with a dependency injection framework such as OSGi Blueprint.
 */
public class DefaultGraphDatabaseFactory implements GraphDatabaseFactory {

	private IndexIterable indexIterable;
	
	@Override
	public EmbeddedGraphDatabase createEmbedded(String storeDir) {
		EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(storeDir, new HashMap<String, String>(),
				indexIterable);
		return graphDb;
	}

	@Override
	public EmbeddedReadOnlyGraphDatabase createEmbeddedReadOnly(String storeDir) {
		EmbeddedReadOnlyGraphDatabase graphDb = new EmbeddedReadOnlyGraphDatabase(storeDir, 
				new HashMap<String, String>(), indexIterable);
		return graphDb;
	}

	public IndexIterable getIndexIterable() {
		return indexIterable;
	}

	/**
	 * Sets an {@link IndexProvider} iterable source.
	 * {@link ListIndexIterable} is a flexible provider that works well with
	 * dependency injection.
	 * @param indexIterable It's actually Iterable<IndexProvider>, but internally typecasted
     *     to workaround bug https://issues.apache.org/jira/browse/ARIES-834 .
	 */
	public void setIndexIterable(IndexIterable indexIterable) {
		this.indexIterable = indexIterable;
	}

}
