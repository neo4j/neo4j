/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.parser.common.ast.factory;

public interface SimpleEither<L, R> {
    static <L, R> SimpleEither<L, R> left(L value) {
        return new EitherImpl(value, null);
    }

    static <L, R> SimpleEither<L, R> right(R value) {
        return new EitherImpl(null, value);
    }

    L getLeft();

    R getRight();
}

class EitherImpl<L, R> implements SimpleEither<L, R> {
    private final L left;
    private final R right;

    EitherImpl(L left, R right) {
        if (left == null && right == null) {
            throw new IllegalStateException("no value set for Either");
        }
        this.left = left;
        this.right = right;
    }

    @Override
    public L getLeft() {
        return left;
    }

    @Override
    public R getRight() {
        return right;
    }
}
