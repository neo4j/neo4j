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
package org.neo4j.internal.recordstorage;

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public enum RelationshipConnection {
    START_PREV {
        @Override
        public long get(RelationshipRecord rel) {
            return rel.isFirstInFirstChain() ? Record.NO_NEXT_RELATIONSHIP.intValue() : rel.getFirstPrevRel();
        }

        @Override
        public long getRaw(RelationshipRecord rel) {
            return rel.getFirstPrevRel();
        }

        @Override
        public void set(RelationshipRecord rel, long id, boolean isFirst) {
            rel.setFirstPrevRel(id);
            rel.setFirstInFirstChain(isFirst);
        }

        @Override
        public RelationshipConnection otherSide() {
            return START_NEXT;
        }

        @Override
        public long compareNode(RelationshipRecord rel) {
            return rel.getFirstNode();
        }

        @Override
        public RelationshipConnection start() {
            return this;
        }

        @Override
        public RelationshipConnection end() {
            return END_PREV;
        }

        @Override
        public boolean isFirstInChain(RelationshipRecord rel) {
            return rel.isFirstInFirstChain();
        }
    },
    START_NEXT {
        @Override
        public long get(RelationshipRecord rel) {
            return rel.getFirstNextRel();
        }

        @Override
        public long getRaw(RelationshipRecord rel) {
            return rel.getFirstNextRel();
        }

        @Override
        public void set(RelationshipRecord rel, long id, boolean isFirst) {
            rel.setFirstNextRel(id);
        }

        @Override
        public RelationshipConnection otherSide() {
            return START_PREV;
        }

        @Override
        public long compareNode(RelationshipRecord rel) {
            return rel.getFirstNode();
        }

        @Override
        public RelationshipConnection start() {
            return this;
        }

        @Override
        public RelationshipConnection end() {
            return END_NEXT;
        }

        @Override
        public boolean isFirstInChain(RelationshipRecord rel) {
            return rel.isFirstInFirstChain();
        }
    },
    END_PREV {
        @Override
        public long get(RelationshipRecord rel) {
            return rel.isFirstInSecondChain() ? Record.NO_NEXT_RELATIONSHIP.intValue() : rel.getSecondPrevRel();
        }

        @Override
        public long getRaw(RelationshipRecord rel) {
            return rel.getSecondPrevRel();
        }

        @Override
        public void set(RelationshipRecord rel, long id, boolean isFirst) {
            rel.setSecondPrevRel(id);
            rel.setFirstInSecondChain(isFirst);
        }

        @Override
        public RelationshipConnection otherSide() {
            return END_NEXT;
        }

        @Override
        public long compareNode(RelationshipRecord rel) {
            return rel.getSecondNode();
        }

        @Override
        public RelationshipConnection start() {
            return START_PREV;
        }

        @Override
        public RelationshipConnection end() {
            return this;
        }

        @Override
        public boolean isFirstInChain(RelationshipRecord rel) {
            return rel.isFirstInSecondChain();
        }
    },
    END_NEXT {
        @Override
        public long get(RelationshipRecord rel) {
            return rel.getSecondNextRel();
        }

        @Override
        public long getRaw(RelationshipRecord rel) {
            return rel.getSecondNextRel();
        }

        @Override
        public void set(RelationshipRecord rel, long id, boolean isFirst) {
            rel.setSecondNextRel(id);
        }

        @Override
        public RelationshipConnection otherSide() {
            return END_PREV;
        }

        @Override
        public long compareNode(RelationshipRecord rel) {
            return rel.getSecondNode();
        }

        @Override
        public RelationshipConnection start() {
            return START_NEXT;
        }

        @Override
        public RelationshipConnection end() {
            return this;
        }

        @Override
        public boolean isFirstInChain(RelationshipRecord rel) {
            return rel.isFirstInSecondChain();
        }
    };

    public abstract long get(RelationshipRecord rel);

    public abstract long getRaw(RelationshipRecord rel);

    public abstract boolean isFirstInChain(RelationshipRecord rel);

    public abstract void set(RelationshipRecord rel, long id, boolean isFirst);

    public abstract long compareNode(RelationshipRecord rel);

    public abstract RelationshipConnection otherSide();

    public abstract RelationshipConnection start();

    public abstract RelationshipConnection end();
}
