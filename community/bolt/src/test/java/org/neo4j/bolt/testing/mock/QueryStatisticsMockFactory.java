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
package org.neo4j.bolt.testing.mock;

import java.util.function.Consumer;
import org.neo4j.graphdb.QueryStatistics;

public final class QueryStatisticsMockFactory extends AbstractMockFactory<QueryStatistics, QueryStatisticsMockFactory> {

    private QueryStatisticsMockFactory() {
        super(QueryStatistics.class);
    }

    public static QueryStatisticsMockFactory newFactory() {
        return new QueryStatisticsMockFactory();
    }

    public static QueryStatistics newInstance() {
        return newFactory().build();
    }

    public static QueryStatistics newInstance(Consumer<QueryStatisticsMockFactory> configurer) {
        var factory = newFactory();
        configurer.accept(factory);
        return factory.newInstance();
    }

    public QueryStatisticsMockFactory withNodesCreated(int nodesCreated) {
        if (nodesCreated != 0) {
            this.withContainsUpdates(true);
        }
        return this.withStaticValue(QueryStatistics::getNodesCreated, nodesCreated);
    }

    public QueryStatisticsMockFactory withNodesDeleted(int nodesDeleted) {
        if (nodesDeleted != 0) {
            this.withContainsUpdates(true);
        }
        return this.withStaticValue(QueryStatistics::getNodesDeleted, nodesDeleted);
    }

    public QueryStatisticsMockFactory withRelationshipsCreated(int relationshipsCreated) {
        if (relationshipsCreated != 0) {
            this.withContainsUpdates(true);
        }
        return this.withStaticValue(QueryStatistics::getRelationshipsCreated, relationshipsCreated);
    }

    public QueryStatisticsMockFactory withRelationshipsDeleted(int relationshipsDeleted) {
        if (relationshipsDeleted != 0) {
            this.withContainsUpdates(true);
        }
        return this.withStaticValue(QueryStatistics::getRelationshipsDeleted, relationshipsDeleted);
    }

    public QueryStatisticsMockFactory withPropertiesSet(int propertiesSet) {
        if (propertiesSet != 0) {
            this.withContainsUpdates(true);
        }
        return this.withStaticValue(QueryStatistics::getPropertiesSet, propertiesSet);
    }

    public QueryStatisticsMockFactory withLabelsAdded(int labelsAdded) {
        if (labelsAdded != 0) {
            this.withContainsUpdates(true);
        }
        return this.withStaticValue(QueryStatistics::getLabelsAdded, labelsAdded);
    }

    public QueryStatisticsMockFactory withLabelsRemoved(int labelsRemoved) {
        if (labelsRemoved != 0) {
            this.withContainsUpdates(true);
        }
        return this.withStaticValue(QueryStatistics::getLabelsRemoved, labelsRemoved);
    }

    public QueryStatisticsMockFactory withIndexesAdded(int indexesAdded) {
        return this.withStaticValue(QueryStatistics::getIndexesAdded, indexesAdded);
    }

    public QueryStatisticsMockFactory withIndexesRemoved(int indexesRemoved) {
        return this.withStaticValue(QueryStatistics::getIndexesRemoved, indexesRemoved);
    }

    public QueryStatisticsMockFactory withConstraintsAdded(int constraintsAdded) {
        return this.withStaticValue(QueryStatistics::getConstraintsAdded, constraintsAdded);
    }

    public QueryStatisticsMockFactory withConstraintsRemoved(int constraintsRemoved) {
        return this.withStaticValue(QueryStatistics::getConstraintsRemoved, constraintsRemoved);
    }

    public QueryStatisticsMockFactory withSystemUpdates(int systemUpdates) {
        if (systemUpdates != 0) {
            this.withContainsSystemUpdates(true);
        } else {
            this.withContainsSystemUpdates(false);
        }
        return this.withStaticValue(QueryStatistics::getSystemUpdates, systemUpdates);
    }

    public QueryStatisticsMockFactory withContainsUpdates(boolean containsUpdates) {
        return this.withStaticValue(QueryStatistics::containsUpdates, containsUpdates);
    }

    public QueryStatisticsMockFactory withContainsSystemUpdates(boolean containsSystemUpdates) {
        return this.withStaticValue(QueryStatistics::getSystemUpdates, containsSystemUpdates);
    }
}
