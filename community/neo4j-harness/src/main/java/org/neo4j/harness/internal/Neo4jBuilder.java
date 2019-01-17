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
package org.neo4j.harness.internal;

import org.neo4j.harness.junit.Neo4j;

/**
 * Builder for constructing and starting Neo4j for test purposes.
 */
public interface Neo4jBuilder extends Neo4jConfigurator<Neo4jBuilder>
{
    /**
     * Start new neo4j instance. By default, the neo4j server will listen on random free port, and you can determine where to
     * connect using the {@link Neo4j#httpURI()} method. You could also specify explicit ports using the
     * {@link #withConfig(org.neo4j.graphdb.config.Setting, String)} method or disable web server completely. Please refer to the Neo4j Manual for
     * details on available configuration options.
     */
    InProcessNeo4j build();
}
