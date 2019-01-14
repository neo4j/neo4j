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
package org.neo4j.graphdb.factory;

import org.junit.Test;

import java.util.Collections;

import org.neo4j.kernel.DummyExtensionFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterables.count;

public class GraphDatabaseFactoryStateTest
{
    @Test
    public void mustBeAbleToRemoveAddedKernelExtensions()
    {
        DummyExtensionFactory extensionFactory = new DummyExtensionFactory();
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        long initialCount = count( state.getKernelExtension() );

        state.addKernelExtensions( Collections.singleton( extensionFactory ) );
        assertThat( count( state.getKernelExtension() ), is( initialCount + 1 ) );

        state.removeKernelExtensions( e -> e == extensionFactory );
        assertThat( count( state.getKernelExtension() ), is( initialCount ) );
    }
}
