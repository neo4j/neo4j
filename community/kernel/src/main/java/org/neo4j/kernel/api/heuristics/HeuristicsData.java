package org.neo4j.kernel.api.heuristics;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.statistics.LabelledDistribution;

public interface HeuristicsData {
    /** Label id -> relative occurrence, value between 0 and 1. The total may be > 1, since labels may co-occur. */
    LabelledDistribution<Integer> labelDistribution();

    /** Relationship type id -> relative occurrence, value between 0 and 1. The total adds up to 1 */
    LabelledDistribution<Integer> relationshipTypeDistribution();

    /** Relationship degree distribution for a label/rel type/direction triplet. */
    double degree( int labelId, int relType, Direction direction );

    /** Ratio of live nodes (i.e. nodes that are not deleted or corrupted) of all addressable nodes */
    double liveNodesRatio();

    /** Maximum number of addressable nodes */
    long maxAddressableNodes();
}
