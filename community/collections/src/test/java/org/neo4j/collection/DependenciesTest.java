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
package org.neo4j.collection;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.collection.factory.OnHeapCollectionsFactory;
import org.neo4j.exceptions.UnsatisfiedDependencyException;

class DependenciesTest {
    @Test
    void givenSatisfiedTypeWhenResolveWithTypeThenInstanceReturned() {
        // Given
        Dependencies dependencies = new Dependencies();

        String foo = "foo";
        dependencies.satisfyDependency(foo);

        // When
        String instance = dependencies.resolveDependency(String.class);

        // Then
        assertThat(instance).isEqualTo(foo);
    }

    @Test
    void givenSatisfiedTypeWhenResolveWithSuperTypeThenInstanceReturned() {
        // Given
        Dependencies dependencies = new Dependencies();

        AbstractList foo = new ArrayList();
        dependencies.satisfyDependency(foo);

        // When
        AbstractList instance = dependencies.resolveDependency(AbstractList.class);

        // Then
        assertThat(instance).isEqualTo(foo);
    }

    @Test
    void givenSatisfiedTypeWhenResolveWithInterfaceThenInstanceReturned() {
        // Given
        Dependencies dependencies = new Dependencies();

        List foo = new ArrayList();
        dependencies.satisfyDependency(foo);

        // When
        List instance = dependencies.resolveDependency(List.class);

        // Then
        assertThat(instance).isEqualTo(foo);
    }

    @Test
    void givenSatisfiedTypeWhenResolveWithSubInterfaceThenInstanceReturned() {
        // Given
        Dependencies dependencies = new Dependencies();

        Collection foo = new ArrayList();
        dependencies.satisfyDependency(foo);

        // When
        Collection instance = dependencies.resolveDependency(Collection.class);

        // Then
        assertThat(instance).isEqualTo(foo);
    }

    @Test
    void givenSatisfiedTypeInParentWhenResolveWithTypeInEmptyDependenciesThenInstanceReturned() {
        // Given
        Dependencies parent = new Dependencies();
        Dependencies dependencies = new Dependencies(parent);

        Collection foo = new ArrayList();
        dependencies.satisfyDependency(foo);

        // When
        Collection instance = dependencies.resolveDependency(Collection.class);

        // Then
        assertThat(instance).isEqualTo(foo);
    }

    @Test
    void givenSatisfiedTypeInParentAndDependenciesWhenResolveWithTypeInDependenciesThenInstanceReturned() {
        // Given
        Dependencies parent = new Dependencies();
        Dependencies dependencies = new Dependencies(parent);

        Collection foo = new ArrayList();
        dependencies.satisfyDependency(foo);
        parent.satisfyDependency(new ArrayList());

        // When
        Collection instance = dependencies.resolveDependency(Collection.class);

        // Then
        assertThat(instance).isEqualTo(foo);
    }

    @Test
    void givenEmptyDependenciesWhenResolveWithTypeThenException() {
        Dependencies dependencies = new Dependencies();

        assertThatThrownBy(() -> dependencies.resolveDependency(Collection.class))
                .isInstanceOf(UnsatisfiedDependencyException.class);
    }

    @Test
    void failSelectFromMultipleAvailableOptions() {
        Dependencies dependencies = new Dependencies();

        List foo = new ArrayList();
        List bar = singletonList("a");
        dependencies.satisfyDependency(foo);
        dependencies.satisfyDependency(bar);

        assertThatThrownBy(() -> dependencies.resolveDependency(List.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resolveOptionalDependency() {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency(OnHeapCollectionsFactory.INSTANCE);

        assertThat(dependencies.resolveOptionalDependency(CollectionsFactory.class))
                .isPresent()
                .hasValue(OnHeapCollectionsFactory.INSTANCE);
        assertThat(dependencies.resolveOptionalDependency(RawIterator.class)).isEmpty();
    }

    @Test
    void resolveOptionalDependencyFromParent() {
        Dependencies parent = new Dependencies();
        parent.satisfyDependency(OnHeapCollectionsFactory.INSTANCE);
        var localDependencies = new Dependencies(parent);

        assertThat(localDependencies.resolveOptionalDependency(CollectionsFactory.class))
                .isPresent()
                .hasValue(OnHeapCollectionsFactory.INSTANCE);
        assertThat(localDependencies.resolveOptionalDependency(RawIterator.class))
                .isEmpty();
    }
}
