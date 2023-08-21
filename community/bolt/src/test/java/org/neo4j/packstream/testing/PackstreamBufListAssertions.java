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
package org.neo4j.packstream.testing;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.packstream.io.PackstreamBuf;

public class PackstreamBufListAssertions
        extends AbstractListAssert<
                PackstreamBufListAssertions, List<? extends PackstreamBuf>, PackstreamBuf, PackstreamBufAssertions> {

    PackstreamBufListAssertions(List<? extends PackstreamBuf> buffers) {
        super(buffers, PackstreamBufListAssertions.class);
    }

    public static PackstreamBufListAssertions assertThat(List<? extends PackstreamBuf> value) {
        return new PackstreamBufListAssertions(value);
    }

    @SuppressWarnings("rawtypes") // That's how AssertJ works, apparently ...
    public static InstanceOfAssertFactory<List, PackstreamBufListAssertions> packstreamBufList() {
        return new InstanceOfAssertFactory<>(List.class, PackstreamBufListAssertions::new);
    }

    @Override
    protected PackstreamBufAssertions toAssert(PackstreamBuf value, String description) {
        return new PackstreamBufAssertions(value).describedAs(description);
    }

    @Override
    protected PackstreamBufListAssertions newAbstractIterableAssert(Iterable<? extends PackstreamBuf> iterable) {
        var buffers = StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());

        return new PackstreamBufListAssertions(buffers);
    }
}
