package org.apache.lucene;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;

/** A ranked list of documents, used to hold search results.
 * <p>
 * <b>Caution:</b> Iterate only over the hits needed.  Iterating over all
 * hits is generally not desirable and may be the source of
 * performance issues. If you need to iterate over many or all hits, consider
 * using the search method that takes a {@link HitCollector}.
 * </p>
 * <p><b>Note:</b> Deleting matching documents concurrently with traversing 
 * the hits, might, when deleting hits that were not yet retrieved, decrease
 * {@link #length()}. In such case, 
 * {@link java.util.ConcurrentModificationException ConcurrentModificationException}
 * is thrown when accessing hit <code>n</code> &ge; current_{@link #length()} 
 * (but <code>n</code> &lt; {@link #length()}_at_start). 
 * 
 * see {@link Searcher#search(Query, int)}, {@link Searcher#search(Query, Filter, int)}
 * and {@link Searcher#search(Query, Filter, int, Sort)}:<br>
 * <pre>
 *   TopDocs topDocs = searcher.search(query, numHits);
 *   ScoreDoc[] hits = topDocs.scoreDocs;
 *   for (int i = 0; i < hits.length; i++) {
 *     int docId = hits[i].doc;
 *     Document d = searcher.doc(docId);
 *     // do something with current hit
 *     ...
 * </pre>
 */

// NOTE: This is the Hits class from lucene 2.x, it was removed in 3.x and was
// used for iterating over all the hits from a query result, not just the N
// top docs.
public final class Hits {
  private Weight weight;
  private Searcher searcher;
  private Filter filter = null;
  private Sort sort = null;

  private int length;                 // the total number of hits
  private Vector<HitDoc> hitDocs = new Vector<HitDoc>();      // cache of hits retrieved

  private HitDoc first;         // head of LRU cache
  private HitDoc last;          // tail of LRU cache
  private int numDocs = 0;      // number cached
  private int maxDocs = 200;    // max to cache
  
  private int nDeletions;       // # deleted docs in the index.    
  private int lengthAtStart;    // this is the number apps usually count on (although deletions can bring it down). 
  private int nDeletedHits = 0; // # of already collected hits that were meanwhile deleted.

  boolean debugCheckedForDeletions = false; // for test purposes.

  public Hits(Searcher s, Query q, Filter f) throws IOException {
    weight = q.weight(s);
    searcher = s;
    filter = f;
    nDeletions = countDeletions(s);
    getMoreDocs(50); // retrieve 100 initially
    lengthAtStart = length;
  }

  public Hits(Searcher s, Query q, Filter f, Sort o) throws IOException {
    weight = q.weight(s);
    searcher = s;
    filter = f;
    sort = o;
    nDeletions = countDeletions(s);
    getMoreDocs(50); // retrieve 100 initially
    lengthAtStart = length;
  }

  // count # deletions, return -1 if unknown.
  private int countDeletions(Searcher s) throws IOException {
    int cnt = -1;
    if (s instanceof IndexSearcher) {
      cnt = s.maxDoc() - ((IndexSearcher) s).getIndexReader().numDocs(); 
    } 
    return cnt;
  }

  /**
   * Tries to add new documents to hitDocs.
   * Ensures that the hit numbered <code>min</code> has been retrieved.
   */
  private final void getMoreDocs(int min) throws IOException {
    if (hitDocs.size() > min) {
      min = hitDocs.size();
    }

    int n = min * 2;    // double # retrieved
    TopDocs topDocs = (sort == null) ? searcher.search(weight, filter, n) : searcher.search(weight, filter, n, sort);
    
    length = topDocs.totalHits;
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;

    float scoreNorm = 1.0f;
    
    if (length > 0 && topDocs.getMaxScore() > 1.0f) {
      scoreNorm = 1.0f / topDocs.getMaxScore();
    }

    int start = hitDocs.size() - nDeletedHits;

    // any new deletions?
    int nDels2 = countDeletions(searcher);
    debugCheckedForDeletions = false;
    if (nDeletions < 0 || nDels2 > nDeletions) { 
      // either we cannot count deletions, or some "previously valid hits" might have been deleted, so find exact start point
      nDeletedHits = 0;
      debugCheckedForDeletions = true;
      int i2 = 0;
      for (int i1=0; i1<hitDocs.size() && i2<scoreDocs.length; i1++) {
        int id1 = ((HitDoc)hitDocs.get(i1)).id;
        int id2 = scoreDocs[i2].doc;
        if (id1 == id2) {
          i2++;
        } else {
          nDeletedHits ++;
        }
      }
      start = i2;
    }

    int end = scoreDocs.length < length ? scoreDocs.length : length;
    length += nDeletedHits;
    for (int i = start; i < end; i++) {
      hitDocs.addElement(new HitDoc(scoreDocs[i].score * scoreNorm,
                                    scoreDocs[i].doc));
    }
    
    nDeletions = nDels2;
  }

  /** Returns the total number of hits available in this set. */
  public final int length() {
    return length;
  }

  /** Returns the stored fields of the n<sup>th</sup> document in this set.
   * <p>Documents are cached, so that repeated requests for the same element may
   * return the same Document object.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public final Document doc(int n) throws CorruptIndexException, IOException {
    HitDoc hitDoc = hitDoc(n);

    // Update LRU cache of documents
    remove(hitDoc);               // remove from list, if there
    addToFront(hitDoc);           // add to front of list
    if (numDocs > maxDocs) {      // if cache is full
      HitDoc oldLast = last;
      remove(last);             // flush last
      oldLast.doc = null;       // let doc get gc'd
    }

    if (hitDoc.doc == null) {
      hitDoc.doc = searcher.doc(hitDoc.id);  // cache miss: read document
    }

    return hitDoc.doc;
  }

  /** Returns the score for the n<sup>th</sup> document in this set. */
  public final float score(int n) throws IOException {
    return hitDoc(n).score;
  }

  /** Returns the id for the n<sup>th</sup> document in this set.
   * Note that ids may change when the index changes, so you cannot
   * rely on the id to be stable.
   */
  public final int id(int n) throws IOException {
    return hitDoc(n).id;
  }

  /**
   * Returns a {@link HitIterator} to navigate the Hits.  Each item returned
   * from {@link Iterator#next()} is a {@link Hit}.
   * <p>
   * <b>Caution:</b> Iterate only over the hits needed.  Iterating over all
   * hits is generally not desirable and may be the source of
   * performance issues. If you need to iterate over many or all hits, consider
   * using a search method that takes a {@link HitCollector}.
   * </p>
   */
//  public Iterator iterator() {
//    return new HitIterator(this);
//  }

  private final HitDoc hitDoc(int n) throws IOException {
    if (n >= lengthAtStart) {
      throw new IndexOutOfBoundsException("Not a valid hit number: " + n);
    }

    if (n >= hitDocs.size()) {
      getMoreDocs(n);
    }

    if (n >= length) {
      throw new ConcurrentModificationException("Not a valid hit number: " + n);
    }
    
    return (HitDoc) hitDocs.elementAt(n);
  }

  private final void addToFront(HitDoc hitDoc) {  // insert at front of cache
    if (first == null) {
      last = hitDoc;
    } else {
      first.prev = hitDoc;
    }

    hitDoc.next = first;
    first = hitDoc;
    hitDoc.prev = null;

    numDocs++;
  }

  private final void remove(HitDoc hitDoc) {      // remove from cache
    if (hitDoc.doc == null) {     // it's not in the list
      return;                     // abort
    }

    if (hitDoc.next == null) {
      last = hitDoc.prev;
    } else {
      hitDoc.next.prev = hitDoc.prev;
    }

    if (hitDoc.prev == null) {
      first = hitDoc.next;
    } else {
      hitDoc.prev.next = hitDoc.next;
    }

    numDocs--;
  }
}

final class HitDoc {
  float score;
  int id;
  Document doc = null;

  HitDoc next;  // in doubly-linked cache
  HitDoc prev;  // in doubly-linked cache

  HitDoc(float s, int i) {
    score = s;
    id = i;
  }
}
