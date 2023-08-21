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
package org.neo4j.capabilities;

import java.util.ServiceLoader;
import org.neo4j.annotations.service.Service;

/**
 * A placeholder interface to identify capability declarations in the class path using Service Provider Interface mechanism (as defined by java.util.{@link
 * ServiceLoader}.
 * <p>
 * All capabilities must be declared as public static final.
 */
@Service
public interface CapabilityDeclaration {}
