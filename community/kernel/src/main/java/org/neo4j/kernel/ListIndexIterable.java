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
package org.neo4j.kernel;

import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.index.IndexIterable;
import org.neo4j.graphdb.index.IndexProvider;

/**
 * Provides {@link IndexProvider} objects based on a provided list.
 * This fits well with modern dependency injection frameworks including OSGi Blueprint.
 * 
 * @todo This is a helper class to help transition from {@link LegacyIndexIterable}.
 *    When the transition is finished both classes should be replaced with a single
 *    List<IndexProvider> or, better yet, direct dependency injection. 
 * @author ceefour
 */
public class ListIndexIterable implements IndexIterable {

	private List<IndexProvider> indexProviders;
	
	@Override
	public Iterator<IndexProvider> iterator() {
		if (indexProviders != null)
			return indexProviders.iterator();
		else
			throw new IllegalArgumentException("indexProviders list must be set");
	}

	public List<IndexProvider> getIndexProviders() {
		return indexProviders;
	}

	/**
	 * Sets the source of the iterator. This must be called.
	 * @param indexProviders
	 */
	public void setIndexProviders(List<IndexProvider> indexProviders) {
		this.indexProviders = indexProviders;
	}

}
