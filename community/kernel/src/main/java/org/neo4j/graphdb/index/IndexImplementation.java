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
package org.neo4j.graphdb.index;

import java.util.Map;

import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * A provider which can create and instantiate {@link Index}s.
 * An {@link IndexImplementation} is typically tied to one implementation, f.ex.
 * lucene, http://lucene.apache.org/java.
 */
// TODO Rename to LegacyIndexFactorySPI
public interface IndexImplementation extends Lifecycle
{
    /**
     * @param configuration that return a legacy index SPI for.
     * @return a {@link LegacyIndexSPI} which represents a type of index suitable for the
     * given configuration.
     */
    LegacyIndexProviderTransaction newTransaction( IndexCommandFactory commandFactory );

    /**
     * @return an index applier that will get notifications about commands to apply.
     */
    NeoCommandHandler newApplier();

    /**
     * Fills in default configuration parameters for indexes provided from this
     * index provider.
     * @param config the configuration map to complete with defaults.
     * @return a {@link Map} filled with decent defaults for an index from
     * this index provider.
     */
    Map<String, String> fillInDefaults( Map<String, String> config );

    boolean configMatches( Map<String, String> storedConfig, Map<String, String> config );
}
