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
package org.neo4j.io.pagecache.harness;

import java.nio.file.OpenOption;
import java.util.List;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.randomharness.Command;

// TODO mvcc make harness test to work in mixed mode with files mapped using different options at the same time
public class MuninnPageCacheHarnessMultiVersionTest extends MuninnPageCacheHarnessTest {

    @Override
    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.of(PageCacheOpenOptions.MULTI_VERSIONED);
    }

    @Override
    List<Command> additionalDisabledCommands() {
        // WriteMulti command runs command within scope of write cursor of outer command
        // This can result in several unrelated cursors trying to pin the same page which is not possible in
        // multiversioned mode as it uses exclusive write locks on pages
        return List.of(Command.WriteMulti);
    }
}
