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
package org.neo4j.storageengine.api;

/**
 * Creator of {@link Token} instances.
 *
 * @param <TOKEN> {@link Token} instances this factory creates.
 */
public interface TokenFactory<TOKEN extends Token>
{
    /**
     * Instantiates a {@link Token} for a given name and id.
     *
     * @param name token name.
     * @param id token id.
     * @return {@link Token} instance.
     */
    TOKEN newToken( String name, int id );
}
