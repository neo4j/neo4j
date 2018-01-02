/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Vector;

/** A ranked list of documents, used to hold search results.
 * <p>
 * <b>Caution:</b> Iterate only over the hits needed.  Iterating over all
 * hits is generally not desirable and may be the source of
 * performance issues.
 * </p>
 * <p><b>Note:</b> Deleting matching documents concurrently with traversing
 * the hits, might, when deleting hits that were not yet retrieved, decrease
 * {@link #length()}. In such case,
 * {@link java.util.ConcurrentModificationException ConcurrentModificationException}
 * is thrown when accessing hit <code>n</code> &ge; current_{@link #length()}
 * (but <code>n</code> &lt; {@link #length()}_at_start).
 *
 * see {@link org.apache.lucene.search.Searcher#search(org.apache.lucene.search.Query , int)}, {@link org.apache.lucene.search.Searcher#search(org.apache.lucene.search.Query , org.apache.lucene.search.Filter , int)}
 * and {@link org.apache.lucene.search.Searcher#search(org.apache.lucene.search.Query , org.apache.lucene.search.Filter , int, org.apache.lucene.search.Sort)}:<br>
 * <pre>
 *   TopDocs topDocs = searcher.search(query, numHits);
 *   ScoreDoc[] hits = topDocs.scoreDocs;
 *   for (int i = 0; i &lt; hits.length; i++) {
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
  private static int MAX_CACHED_DOCS = 200;    // max to cache

  private final Weight weight;
  private final IndexSearcher searcher;
  private Filter filter = null;
  private Sort sort = null;

  private int length;                 // the total number of hits
  private final Vector<HitDoc> hitDocs = new Vector<HitDoc>();      // cache of hits retrieved

  private HitDoc first;         // head of LRU cache
  private HitDoc last;          // tail of LRU cache
  private int numDocs = 0;      // number cached

  private int nDeletions;       // # deleted docs in the index.
  private final int lengthAtStart;    // this is the number apps usually count on (although deletions can bring it down).
  private int nDeletedHits = 0; // # of already collected hits that were meanwhile deleted.

  private final boolean score;

  public Hits(IndexSearcher s, Query q, Filter f) throws IOException
  {
    score = false;
    weight = q.weight(s);
    searcher = s;
    filter = f;
    nDeletions = countDeletions(s);
    getMoreDocs(50); // retrieve 100 initially
    lengthAtStart = length;
  }

  public Hits(IndexSearcher s, Query q, Filter f, Sort o, boolean score) throws IOException {
    this.score = score;
    weight = q.weight(s);
    searcher = s;
    filter = f;
    sort = o;
    nDeletions = countDeletions(s);
    getMoreDocs(50); // retrieve 100 initially
    lengthAtStart = length;
  }

  // count # deletions, return -1 if unknown.
  private int countDeletions(IndexSearcher s) {
    int cnt = -1;
    if ( s != null ) {
      cnt = s.maxDoc() - s.getIndexReader().numDocs();
    }
    return cnt;
  }

  /**
   * Tries to add new documents to hitDocs.
   * Ensures that the hit numbered <code>min</code> has been retrieved.
   */
  private void getMoreDocs(int min) throws IOException {
    if (hitDocs.size() > min) {
      min = hitDocs.size();
    }

    int n = min * 2;    // double # retrieved
//  TopDocs topDocs = (sort == null) ? searcher.search(weight, filter, n) : searcher.search(weight, filter, n, sort);
    TopDocs topDocs = null;
    if ( sort == null )
    {
        topDocs = searcher.search( weight, filter, n );
    }
    else
    {
        if ( this.score )
        {
            TopFieldCollector collector = LuceneDataSource.scoringCollector( sort, n );
            searcher.search( weight, null, collector );
            topDocs = collector.topDocs();
        }
        else
        {
            topDocs = searcher.search( weight, filter, n, sort );
        }
    }


    length = topDocs.totalHits;
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;

    float scoreNorm = 1.0f;

    if (length > 0 && topDocs.getMaxScore() > 1.0f) {
      scoreNorm = 1.0f / topDocs.getMaxScore();
    }

    int start = hitDocs.size() - nDeletedHits;

    // any new deletions?
    int nDels2 = countDeletions(searcher);
    if (nDeletions < 0 || nDels2 > nDeletions) {
      // either we cannot count deletions, or some "previously valid hits" might have been deleted, so find exact start point
      nDeletedHits = 0;
      int i2 = 0;
      for (int i1=0; i1<hitDocs.size() && i2<scoreDocs.length; i1++) {
        int id1 = hitDocs.get(i1).id;
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

    /** Returns the total number of hits available in this set.
     * 
     * @return the total number of hits available in this set
     */
  public int length() {
    return length;
  }

  /** Returns the stored fields of the n<sup>th</sup> document in this set.
   * <p>
   * Documents are cached, so that repeated requests for the same element may
   * return the same Document object.
   * 
   * @param n the index of the document to get
   * @return the stored fields of the document
   * @throws org.apache.lucene.index.CorruptIndexException if the index is corrupt
   * @throws java.io.IOException if there is a low-level IO error
   */
  public Document doc(int n) throws CorruptIndexException, IOException {
    HitDoc hitDoc = hitDoc(n);

    // Update LRU cache of documents
    remove(hitDoc);               // remove from list, if there
    addToFront(hitDoc);           // add to front of list
    if (numDocs > MAX_CACHED_DOCS ) {      // if cache is full
      HitDoc oldLast = last;
      remove(last);             // flush last
      oldLast.doc = null;       // let doc get gc'd
    }

    if (hitDoc.doc == null) {
        hitDoc.doc = searcher.doc(hitDoc.id);  // cache miss: read document
    }

    return hitDoc.doc;
  }

  /** Returns the score for the n<sup>th</sup> document in this set.
   *
   * @param n the index of the document
   * @return the score for the document
   * @throws java.io.IOException if there is a low-level IO error
   */
  public float score(int n) throws IOException {
    return hitDoc(n).score;
  }

  private HitDoc hitDoc(int n) throws IOException {
    if (n >= lengthAtStart) {
      throw new IndexOutOfBoundsException("Not a valid hit number: " + n);
    }

    if (n >= hitDocs.size()) {
      getMoreDocs(n);
    }

    if (n >= length) {
      throw new ConcurrentModificationException("Not a valid hit number: " + n);
    }

    return hitDocs.elementAt(n);
  }

  private void addToFront(HitDoc hitDoc) {  // insert at front of cache
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

  private void remove(HitDoc hitDoc) {      // remove from cache
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

  org.neo4j.index.impl.lucene.HitDoc next;  // in doubly-linked cache
  org.neo4j.index.impl.lucene.HitDoc prev;  // in doubly-linked cache

  HitDoc(float s, int i) {
    score = s;
    id = i;
  }
}
