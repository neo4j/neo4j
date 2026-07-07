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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs;

import java.util.BitSet;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;

public interface SignpostTracking {
    boolean isProtectedFromPruning(SignpostStack stack);

    boolean canAbandonTraceBranch(SignpostStack stack);

    void onPushed(TwoWaySignpost signpost, SignpostStack stack);

    void onPopped(TwoWaySignpost signpost, SignpostStack stack);

    boolean validate(SignpostStack stack);

    void clear();

    static SignpostTracking trailMode(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        return new TrailModeSignPostTracking(memoryTracker, hooks);
    }

    static SignpostTracking acyclicMode(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        return new AcyclicModeSignPostTracking(memoryTracker, hooks);
    }

    static SignpostTracking walkMode() {
        return NO_TRACKING;
    }

    SignpostTracking NO_TRACKING = new SignpostTracking() {
        @Override
        public boolean isProtectedFromPruning(SignpostStack stack) {
            return false;
        }

        @Override
        public boolean canAbandonTraceBranch(SignpostStack stack) {
            return false;
        }

        @Override
        public void onPushed(TwoWaySignpost signpost, SignpostStack stack) {
            // do nothing
        }

        @Override
        public boolean validate(SignpostStack stack) {
            return true;
        }

        @Override
        public void onPopped(TwoWaySignpost signpost, SignpostStack stack) {
            // do nothing
        }

        @Override
        public void clear() {
            // do nothing
        }
    };

    final class TrailModeSignPostTracking implements SignpostTracking {
        private final DepthPresenceTracker relationshipPresence;
        private final BitSet protectFromPruning;
        private final PPBFSHooks hooks;

        TrailModeSignPostTracking(MemoryTracker memoryTracker, PPBFSHooks hooks) {
            this.relationshipPresence = new DepthPresenceTracker(memoryTracker);
            this.protectFromPruning = new BitSet();
            this.hooks = hooks;
        }

        @Override
        public boolean isProtectedFromPruning(SignpostStack stack) {
            return protectFromPruning.get(stack.size());
        }

        /** this function allows us to abandon a trace branch early. if we have detected a duplicate relationship then
         * the set of paths we're currently tracing are all invalid and so we should be able to abort tracing them, except
         * tracing also performs verification/validation.
         *
         * if the current node is validated then further tracing has no benefit, so we can pop back to the previous
         * node.
         * */
        @Override
        public boolean canAbandonTraceBranch(SignpostStack stack) {
            var head = stack.headSignpost();
            if (!(head instanceof TwoWaySignpost.RelSignpost)) return false;
            int dup = relationshipPresence.distanceToDuplicate(((TwoWaySignpost.RelSignpost) head).relId);

            if (dup == 0) {
                return false;
            }

            int sourceLength = stack.lengthFromSource();
            for (int i = 0; i <= dup; i++) {
                var candidate = stack.signpost(stack.size() - 1 - i);

                if (!candidate.prevNode.validatedAtLength(sourceLength)) {
                    return false;
                }

                sourceLength += candidate.dataGraphLength();
            }

            this.protectFromPruning.set(stack.size() - 1 - dup, stack.size() - 1, true);
            return true;
        }

        @Override
        public void onPushed(TwoWaySignpost signpost, SignpostStack stack) {
            int size = stack.size();
            this.protectFromPruning.set(size - 1, false);
            if (signpost instanceof TwoWaySignpost.RelSignpost rel) {
                relationshipPresence.add(rel.relId, size - 1);
            }
        }

        @Override
        public void onPopped(TwoWaySignpost signpost, SignpostStack stack) {
            if (signpost instanceof TwoWaySignpost.RelSignpost rel) {
                relationshipPresence.remove(rel.relId, stack.size());
            }
        }

        @Override
        public boolean validate(SignpostStack stack) {
            int sourceLength = 0;
            for (int i = stack.size() - 1; i >= 0; i--) {
                TwoWaySignpost signpost = stack.signpost(i);
                sourceLength += signpost.dataGraphLength();
                if (signpost instanceof TwoWaySignpost.RelSignpost rel) {
                    assert relationshipPresence.isPresent(rel.relId, i);
                    if (relationshipPresence.isPresentBeyond(rel.relId, i)) {
                        hooks.invalid(stack);
                        return false;
                    }
                }

                validateSignpostLength(signpost, sourceLength, stack);
            }
            return true;
        }

        @Override
        public void clear() {
            relationshipPresence.clear();
        }
    }

    /**
     * Acyclic mode tracking: ensures no node appears more than once in a path.
     * Uses a {@link DepthPresenceTracker} (nodeId to positions) to track
     * where each node appears in the current path, mirroring Trail's relationship tracking.
     *
     * <p>A node's position is its distance from the target, counted in signposts: the target
     * sits at position 0, and the prevNode of signpost i sits at position i + 1.
     */
    final class AcyclicModeSignPostTracking implements SignpostTracking {
        private final DepthPresenceTracker nodePresence;
        private final BitSet protectFromPruning;
        private final PPBFSHooks hooks;

        AcyclicModeSignPostTracking(MemoryTracker memoryTracker, PPBFSHooks hooks) {
            this.nodePresence = new DepthPresenceTracker(memoryTracker);
            this.protectFromPruning = new BitSet();
            this.hooks = hooks;
        }

        @Override
        public boolean isProtectedFromPruning(SignpostStack stack) {
            return protectFromPruning.get(stack.size());
        }

        @Override
        public boolean canAbandonTraceBranch(SignpostStack stack) {
            var head = stack.headSignpost();
            if (!(head instanceof TwoWaySignpost.RelSignpost)) return false;
            int dup = nodePresence.distanceToDuplicate(head.prevNode.id());

            if (dup == 0) {
                return false;
            }

            // If the duplicate is the target (position 0) there is no signpost for it,
            // so cap the walk at the number of signposts on the stack.
            int numSignposts = Math.min(dup + 1, stack.size());
            int sourceLength = stack.lengthFromSource();
            for (int i = 0; i < numSignposts; i++) {
                var candidate = stack.signpost(stack.size() - 1 - i);

                if (!candidate.prevNode.validatedAtLength(sourceLength)) {
                    return false;
                }

                sourceLength += candidate.dataGraphLength();
            }

            this.protectFromPruning.set(stack.size() - numSignposts, stack.size() - 1, true);
            return true;
        }

        @Override
        public void onPushed(TwoWaySignpost signpost, SignpostStack stack) {
            int size = stack.size();
            this.protectFromPruning.set(size - 1, false);
            if (size == 1) {
                // The target is the one node on the path that is never a prevNode, so the branch
                // below never records it. Record it here, at its own position 0.
                nodePresence.add(stack.target().id(), 0);
            }
            if (signpost instanceof TwoWaySignpost.RelSignpost) {
                nodePresence.add(signpost.prevNode.id(), size);
            }
        }

        @Override
        public void onPopped(TwoWaySignpost signpost, SignpostStack stack) {
            if (signpost instanceof TwoWaySignpost.RelSignpost) {
                nodePresence.remove(signpost.prevNode.id(), stack.size() + 1);
            }
            if (stack.size() == 0) {
                nodePresence.remove(stack.target().id(), 0);
            }
        }

        @Override
        public boolean validate(SignpostStack stack) {
            int sourceLength = 0;
            for (int i = stack.size() - 1; i >= 0; i--) {
                TwoWaySignpost signpost = stack.signpost(i);
                sourceLength += signpost.dataGraphLength();

                if (signpost instanceof TwoWaySignpost.RelSignpost) {
                    assert nodePresence.isPresent(signpost.prevNode.id(), i + 1);
                    if (nodePresence.isPresentBeyond(signpost.prevNode.id(), i + 1)) {
                        hooks.invalid(stack);
                        return false;
                    }
                }

                if (i == 0) {
                    // The target has no signpost, so its duplicate check lives here: if it appears
                    // again deeper in the path, the path cycles through the target — reject it.
                    // The check runs at i == 0, not before the loop, so that the length validation
                    // below has already run for every other signpost; other paths that share those
                    // signposts depend on it.
                    if (nodePresence.isPresentBeyond(stack.target().id(), 0)) {
                        hooks.invalid(stack);
                        return false;
                    }
                }

                validateSignpostLength(signpost, sourceLength, stack);
            }
            return true;
        }

        @Override
        public void clear() {
            nodePresence.clear();
            protectFromPruning.clear();
        }
    }

    private static void validateSignpostLength(TwoWaySignpost signpost, int sourceLength, SignpostStack stack) {
        if (!signpost.isValidatedAtLength(sourceLength)) {
            signpost.validate(sourceLength);
            if (!signpost.forwardNode.validatedAtLength(sourceLength)) {
                signpost.forwardNode.setValidatedAtLength(sourceLength, stack.dgLength() - sourceLength);
            }
        }
    }
}
