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
package org.neo4j.bolt.testing.assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;

public class ListValueAssertions
        extends AbstractListAssert<ListValueAssertions, List<? extends AnyValue>, AnyValue, AnyValueAssertions> {

    private ListValueAssertions(List<? extends AnyValue> anyValues) {
        super(anyValues, ListValueAssertions.class);
    }

    public static ListValueAssertions assertThat(List<? extends AnyValue> value) {
        return new ListValueAssertions(value);
    }

    public static ListValueAssertions assertThat(ListValue values) {
        var list = new ArrayList<AnyValue>();
        values.forEach(list::add);

        return new ListValueAssertions(list);
    }

    public static InstanceOfAssertFactory<ListValue, ListValueAssertions> listValue() {
        return new InstanceOfAssertFactory<>(ListValue.class, ListValueAssertions::assertThat);
    }

    @Override
    protected AnyValueAssertions toAssert(AnyValue value, String description) {
        return new AnyValueAssertions(value).as(description);
    }

    @Override
    protected ListValueAssertions newAbstractIterableAssert(Iterable<? extends AnyValue> iterable) {
        var values = StreamSupport.stream(iterable.spliterator(), false).toList();

        return new ListValueAssertions(values);
    }
}
