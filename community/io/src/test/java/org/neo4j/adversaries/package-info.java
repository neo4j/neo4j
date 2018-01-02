/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
/**
 * The package contain implementations of various interfaces, that are useful for robustness, randomised and stress
 * testing.
 *
 * An adversarial implementation of an interface is one that is technically correct, but not well behaved. Their
 * purpose is to push dependers into all kinds of failure modes. Failure modes that would otherwise be highly
 * unlikely to observe in practice, but which the depender must none the less be able to cope with.
 */
package org.neo4j.adversaries;
