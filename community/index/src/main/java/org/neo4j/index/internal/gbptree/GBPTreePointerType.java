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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BYTE_POS_LEFTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BYTE_POS_RIGHTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BYTE_POS_SUCCESSOR;

public sealed interface GBPTreePointerType {
    int offset(InternalNodeBehaviour<?> node);

    static GBPTreePointerType leftSibling() {
        return SimplePointer.LEFT_SIBLING;
    }

    static GBPTreePointerType rightSibling() {
        return SimplePointer.RIGHT_SIBLING;
    }

    static GBPTreePointerType successor() {
        return SimplePointer.SUCCESSOR;
    }

    static GBPTreePointerType noPointer() {
        return SimplePointer.NO_POINTER;
    }

    static GBPTreePointerType child(int pos) {
        return new ChildPointer(pos);
    }

    enum SimplePointer implements GBPTreePointerType {
        NO_POINTER {
            @Override
            public int offset(InternalNodeBehaviour<?> node) {
                return 0;
            }

            @Override
            public String toString() {
                return "no pointer";
            }
        },
        LEFT_SIBLING {
            @Override
            public int offset(InternalNodeBehaviour<?> node) {
                return BYTE_POS_LEFTSIBLING;
            }

            @Override
            public String toString() {
                return "left sibling";
            }
        },
        RIGHT_SIBLING {
            @Override
            public int offset(InternalNodeBehaviour<?> node) {
                return BYTE_POS_RIGHTSIBLING;
            }

            @Override
            public String toString() {
                return "right sibling";
            }
        },
        SUCCESSOR {
            @Override
            public int offset(InternalNodeBehaviour<?> node) {
                return BYTE_POS_SUCCESSOR;
            }

            @Override
            public String toString() {
                return "successor";
            }
        }
    }

    record ChildPointer(int pos) implements GBPTreePointerType {

        @Override
        public int offset(InternalNodeBehaviour<?> node) {
            return node.childOffset(pos);
        }

        @Override
        public String toString() {
            return "child(" + pos + ")";
        }
    }
}
