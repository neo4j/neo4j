/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * <h1>The GBPTree</h1>
 * B+tree implementation with arbitrary key:value entries. Index implementation is
 * {@link org.neo4j.index.internal.gbptree.GBPTree}, which works on a {@link org.neo4j.io.pagecache.PageCache}.
 * Implementation supports single writer together with concurrent lock-free and garbage-free readers.
 * <p>
 * To create an index with a custom layout (type of key:value), implement a custom {@link org.neo4j.index.internal.gbptree.Layout}.
 * <p>
 * See https://en.wikipedia.org/wiki/B%2B_tree
 * <h1>Entries move from left to right</h1>
 * When we make structural changes to the tree we are only allowed to move entries to the right, never to the left.
 * {@link org.neo4j.index.internal.gbptree.SeekCursor Readers} rely on this assumption being true to read correct
 * entries without missing anything. This is also why backwards seek needs to be done with extra care. Moving entries
 * to the left within a page (during remove) is acceptable and readers use {@link org.neo4j.io.pagecache.PageCursor#shouldRetry()}
 * to handle this case.
 * <h1>Split, merge and rebalance</h1>
 * The interesting classes for this section are {@link org.neo4j.index.internal.gbptree.StructurePropagation} and
 * {@link org.neo4j.index.internal.gbptree.InternalTreeLogic}.
 * <p>
 * We can think of the tree as a tree, but we can think of the content as ranges divided by the internal keys.
 * Those two mental models can be useful when thinking about the structure of the tree and we will use them both
 * below. But first some examples illustrating how the tree view and range view fit together:
 * <pre>
 * Simple node:
 * [1 2 3]        1 2 3
 *
 * Small tree with emphasis on range A:
 *       [4]
 *      /  \               4 (divide range A from range B)
 * [1 2 3] [4 5 6]   1 2 3  4 5 6
 *                  A----- B-----
 *
 * Bigger tree with multiple range emphasis:
 *                 [7]
 *                 | |
 *          -------   -----
 *         |               |
 *        [4]             [10]
 *        / \             /  \                        4     7     10
 * [1 2 3] [4 5 6] [7 8 9]  [10 11 12]           1 2 3 4 5 6 7 8 9 10 11 12
 *                                                    A----- (subrange of range B)
 *                                              B----------- (range of a full subtree)
 * </pre>
 * <p>
 * TRAVERSAL PATH - When updating the tree we traverse down some path through the internal tree nodes to target leaf,
 * call this path the "traversal path". When we have performed our update in target leaf we traverse back up along
 * "traversal path" to propagate possible structural changes to the tree, as result of split, merge or rebalance.
 * <p>
 * MERGE
 * <p>
 * We perform a merge between leaf A and B. Those nodes are necessarily siblings, otherwise there could not be
 * a merge between them. Let A be left sibling to B. Leaf A and leaf B belong to different subtrees. Call them
 * "subtree A" and "subtree B". If they share parent subtree A and B only contain leaf A and B respectively.
 * If they don't share parent then their parents are siblings. Each leaf and each subtree contain a range of keys.
 * We can talk about "range of leaf A" or "range of subtree A". If subtree A only contain leaf A then those ranges
 * will be the same.
 * <p>
 * COMMON ANCESTOR - Somewhere along the traversal path there is a common parent to subtree A and subtree B. Call
 * this parent "common ancestor". The range of common ancestor cover both range of leaf A and range of leaf B and
 * it is the first internal node from the bottom of the tree to do this. We use this property to recognize common
 * ancestor when traversing back up along the traversal path.
 * <p>
 * SPLITTER KEY - Because there is sibling relationship(s) between subtree A and B, there must be one single key
 * in common ancestor that split subtree A from subtree B. Call this key the "splitter key". Splitter key act as
 * a guide post directing searches to the correct subtree (as do all keys in internal nodes) and divide range of
 * subtree A from range of subtree B.
 * <p>
 * When merging leaf A and B we move keys from subtree A to subtree B. Or in terms of ranges, we move keys from
 * range of subtree A to range of subtree B. Thus, the splitter key does not any longer accurately divide range
 * of subtree A from range of subtree B. We need to update the splitter key.
 * <p>
 * There are two different scenarios that need to be taken into account:
 * - If leaf A and B share parent, we do not need to divide range of leaf A from range of leaf B because leaf A is
 * gone, merged into B. We simply remove splitter key in parent together with child pointer to the left of splitter
 * key, the one pointing to A.
 * <p>
 * - If leaf A and B don't share parent:
 * <p>
 * BUBBLE KEY and BUBBLE SOURCE - Range of leaf A has been removed from range of subtree A and added to range of
 * subtree B. The key that divided range of leaf A from the rest of the range of subtree A sits to the very right
 * in leaf A's subtree. This means to the very right of leaf A's parent, or parent's parent if parent has no keys,
 * etc. There is no internal key that sits further "to the right" than this one in leaf A's subtree. From the
 * common ancestor's perspective this is "the rightmost internal key in the subtree to the left of old splitter key".
 * Call this key the "bubble key" and the internal node where we found it the "bubble source". The bubble key is the
 * new splitter key that divide subtree A from subtree B and we need to replace old splitter key with this.
 * <p>
 * Replacing the splitter key is not enough however. We also need to remove the bubble key from the bubble
 * source. This might leave bubble source with no keys and only one child. If all of the internal nodes in subtree
 * of leaf A looks like this when we try to find the bubble key then there is no such key to be found and we can
 * safely completely remove this subtree from common ancestor. We can think of this process as the key bubbling up
 * from the bubble source to common ancestor, hence the name.
 * <pre>
 * Pre-merge, note that 13 is splitter key between leaf A and leaf B.
 *             7        13         19
 *        2 4 6 8 10 12    14 16 18  20 22 24
 *              leaf-A-    leaf-B--
 *       subtree-A-----    subtree-B---------
 *
 *  Mid-merge before bubbling new splitter key. Note that 13 (old splitter key) does not sit in the right place in the range.
 *             7        13                 19
 *        2 4 6            8 10 12 14 16 18  20 22 24
 *                         leaf-B----------
 *       subtree-A-----    subtree-B-----------------
 *
 * Post-merge. 7 has bubbled up to become new splitter key
 *                       7                 19
 *        2 4 6            8 10 12 14 16 18  20 22 24
 *                         leaf-B----------
 *        subtree-A-----   subtree-B---------
 * </pre>
 * <p>
 * REBALANCE
 * <p>
 * If we rebalance keys between leaf A and leaf B (assuming that A is left sibling to B) we move keys from leaf A
 * to leaf B, never the other way around. This means we move keys from range of leaf A to range of leaf B. The process
 * of propagating this structure change is very similar to what we do on merge. We need to replace the splitter key
 * with the new leftmost key in leaf B. We do this with a simple replace instead of using the bubble strategy.
 * <p>
 * SPLIT
 * <p>
 * We split a node A into A' and B'. To reflect this in parent we need to insert a new child pointer (to B') and a
 * splitter key that divide range A' from range B'. This key is the leftmost in B'. The splitter key is passed up to
 * parent together with new child pointer to be inserted. Note that even though we pass the splitter key up, we also
 * keep it in B'.
 * <p>
 * When inserting new splitter key and child pointer in parent node it can cause an overflow such that we need to
 * split parent as well. Splitting an internal node is done in a similar fashion to splitting a leaf node with the
 * slight difference that we do not keep the new splitter key among the two internal nodes but instead just pass it
 * up to parent (or parent's parent if you will).
 * <p>
 * This splitting of parents can spread all the way up to the root, in which case we have added a new level in the tree.
 */
package org.neo4j.index.internal.gbptree;
