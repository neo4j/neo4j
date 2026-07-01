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

    /** Called once the stack's target node has been set, before any signposts are pushed. */
    default void onInitialized(SignpostStack stack) {
        // do nothing
    }

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

                if (!signpost.isValidatedAtLength(sourceLength)) {
                    signpost.validate(sourceLength);
                    if (!signpost.forwardNode.validatedAtLength(sourceLength)) {
                        signpost.forwardNode.setValidatedAtLength(sourceLength, stack.dgLength() - sourceLength);
                    }
                }
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
     * <p>Node positions use a +1 offset from signpost indices so the target node fits at position 0:
     * position 0 = target, position S = prevNode of signpost pushed when stack.size() == S.
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
        public void onInitialized(SignpostStack stack) {
            nodePresence.add(stack.target().id(), 0);
        }

        @Override
        public boolean canAbandonTraceBranch(SignpostStack stack) {
            var head = stack.headSignpost();
            if (!(head instanceof TwoWaySignpost.RelSignpost)) return false;
            int dup = nodePresence.distanceToDuplicate(head.prevNode.id());

            if (dup == 0) {
                return false;
            }

            // Cap to stack.size() because the duplicate may be with the target node (position 0),
            // which has no corresponding signpost. In that case dup == stack.size() and dup + 1
            // would exceed the number of signposts on the stack.
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
            // A node signpost at size == 1 indicates the start node, which should also be tracked
            // It would be missed by only looking at relationship signposts
            if (signpost instanceof TwoWaySignpost.RelSignpost || size == 1) {
                nodePresence.add(signpost.prevNode.id(), size);
            }
        }

        @Override
        public void onPopped(TwoWaySignpost signpost, SignpostStack stack) {
            if (signpost instanceof TwoWaySignpost.RelSignpost) {
                nodePresence.remove(signpost.prevNode.id(), stack.size() + 1);
            }
        }

        @Override
        public boolean validate(SignpostStack stack) {
            long targetId = stack.target().id();
            boolean valid = true;
            int sourceLength = 0;
            for (int i = stack.size() - 1; i >= 0; i--) {
                TwoWaySignpost signpost = stack.signpost(i);
                sourceLength += signpost.dataGraphLength();

                if (signpost instanceof TwoWaySignpost.RelSignpost || i == 0) {
                    assert nodePresence.isPresent(signpost.prevNode.id(), i + 1);
                    if (nodePresence.isPresentBeyond(signpost.prevNode.id(), i + 1)) {
                        hooks.invalid(stack);
                        return false;
                    }

                    // Position 1 (i == 0) sits directly against the target and, for quantified
                    // patterns, may be a state-only signpost (no relationship consumed) whose
                    // prevNode is the target itself — that's the accepting-state transition into
                    // the target, not a revisit. Any other position sharing the target's id, or a
                    // genuine relationship signpost at position 1 (e.g. a self-loop), IS a revisit
                    // of the target and must be rejected. Keep bookkeeping for the rest of the
                    // stack going (matching the loop's normal per-signpost side effects below)
                    // rather than returning immediately, since other in-flight traces rely on the
                    // validated-length bookkeeping this loop produces for shared signposts/nodes.
                    boolean isRealStep = signpost instanceof TwoWaySignpost.RelSignpost;
                    if (signpost.prevNode.id() == targetId && (i > 0 || isRealStep)) {
                        hooks.invalid(stack);
                        valid = false;
                    }
                }

                if (!signpost.isValidatedAtLength(sourceLength)) {
                    signpost.validate(sourceLength);
                    if (!signpost.forwardNode.validatedAtLength(sourceLength)) {
                        signpost.forwardNode.setValidatedAtLength(sourceLength, stack.dgLength() - sourceLength);
                    }
                }
            }
            return valid;
        }

        @Override
        public void clear() {
            nodePresence.clear();
            protectFromPruning.clear();
        }
    }
}
