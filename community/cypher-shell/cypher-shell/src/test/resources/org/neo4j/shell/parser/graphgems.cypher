//GRAPHGEMS
//10 (?) cypher queries that will blow your mind

//1. The meta-graph
MATCH (a)-[r]->(b)
WITH labels(a) AS a_labels,type(r) AS rel_type,labels(b) AS b_labels
UNWIND a_labels as l
UNWIND b_labels as l2
MERGE (a:Meta_Node {name:l})
MERGE (b:Meta_Node {name:l2})
MERGE (a)-[:META_RELATIONSHIP {name:rel_type}]->(b)
RETURN distinct l as first_node, rel_type as connected_by, l2 as second_node

//2. Betweenness centrality ftw
MATCH p=allShortestPaths((source:Person)-[:KNOWS*]-(target:Person))
WHERE id(source) < id(target) and length(p) > 1
UNWIND nodes(p)[1..-1] as n
RETURN n.firstname, n.lastname, count(*) as betweenness
ORDER BY betweenness DESC;

//3. Pagerank ftw
//3.a Graph Theory: calculate the PageRank
UNWIND range(1,2) AS round
MATCH (n:Person)
WHERE rand() < 0.1
MATCH (n:Person)-[:KNOWS*..5]->(m:Person)
SET m.rank = coalesce(m.rank,0) + 1

//3.b Graph Theory: Show the PageRank
match (n:Person)
where n.rank is not null
return n.firstname, n.lastname, n.rank
order by n.rank desc
limit 10;

//4. Weighted shortest path
START  startNode=node:node_auto_index(name="Start"),
       endNode=node:node_auto_index(name="Finish")
MATCH  p=(startNode)-[:NAVIGATE_TO*]->(endNode)
RETURN p AS shortestPath,
       reduce(distance=0, r in relationships(p) | distance+r.distance) AS totalDistance
       ORDER BY totalDistance ASC
       LIMIT 1;


//5. Creating an in-graph index based on ordering node properties
match (t:Time)--(d:Day {date: 20150506})
with t
order by t.time ASC
with collect(t) as times
  foreach (i in range(0,length(times)-2) |
    foreach (t1 in [times[i]] |
      foreach (t2 in [times[i+1]] |
        merge (t1)-[:FOLLOWED_BY]->(t2))));

//6. Graph Karaoke
//create the karaoke graph
load csv with headers from "https://docs.google.com/a/neotechnology.com/spreadsheets/d/1Q8W7hDOnkXiLanf2K3P85wAhqQG_qm7bTwI36MbB5fE/export?format=csv&id=1Q8W7hDOnkXiLanf2K3P85wAhqQG_qm7bTwI36MbB5fE&gid=0" as csv
with csv.Sequence as seq, csv.Songsentence as row
unwind row as text
with seq, reduce(t=tolower(text), delim in [",",".","!","?",'"',":",";","'","-"] | replace(t,delim,"")) as normalized
with seq, [w in split(normalized," ") | trim(w)] as words
unwind range(0,size(words)-2) as idx
MERGE (w1:Word {name:words[idx], seq:toInteger(seq)})
MERGE (w2:Word {name:words[idx+1], seq:toInteger(seq)})
MERGE (w1)-[r:NEXT {seq:toInteger(seq)}]->(w2)

match (endword:Word), (startword:Word)
where not ()-[:NEXT]->(startword)
and not (endword)-[:NEXT]->()
and startword.seq=endword.seq+1
merge (endword)-[:NEXTSENTENCE]->(startword)


//7. The Timetree
WITH range(2011, 2014) AS years, range(1,12) as months
FOREACH(year IN years |
  MERGE (y:Year {year: year})
  FOREACH(month IN months |
    CREATE (m:Month {month: month})
    MERGE (y)-[:HAS_MONTH]->(m)
    FOREACH(day IN (CASE
                      WHEN month IN [1,3,5,7,8,10,12] THEN range(1,31)
                      WHEN month = 2 THEN
                        CASE
                          WHEN year % 4 <> 0 THEN range(1,28)
                          WHEN year % 100 <> 0 THEN range(1,29)
                          WHEN year % 400 <> 0 THEN range(1,29)
                          ELSE range(1,28)
                        END
                      ELSE range(1,30)
                    END) |
      CREATE (d:Day {day: day})
      MERGE (m)-[:HAS_DAY]->(d))))

WITH *

MATCH (year:Year)-[:HAS_MONTH]->(month)-[:HAS_DAY]->(day)
WITH year,month,day
ORDER BY year.year, month.month, day.day
WITH collect(day) as days
FOREACH(i in RANGE(0, length(days)-2) |
    FOREACH(day1 in [days[i]] |
        FOREACH(day2 in [days[i+1]] |
            CREATE UNIQUE (day1)-[:NEXT]->(day2))))

//8. Mark's recommendation query
MATCH (me:Person {name: "Adam"})
MATCH (me)-[:FRIEND_OF]-()-[:FRIEND_OF]-(potentialFriend)

WITH me, potentialFriend, COUNT(*) AS friendsInCommon

WITH me,
     potentialFriend,
     SIZE((potentialFriend)-[:LIVES_IN]->()<-[:LIVES_IN]-(me)) AS sameLocation,
     abs( me.age - potentialFriend.age) AS ageDifference,
     LABELS(me) = LABELS(potentialFriend) AS gender,
     friendsInCommon

WHERE NOT (me)-[:FRIEND_OF]-(potentialFriend)

WITH potentialFriend,
       // 100 -> maxScore, 10 -> eightyPercentLevel, friendsInCommon -> score (from the formula above)
       100 * (1 - exp((-1.0 * (log(5.0) / 10)) * friendsInCommon)) AS friendsInCommon,
       sameLocation * 10 AS sameLocation,
       -1 * (10 * (1 - exp((-1.0 * (log(5.0) / 20)) * ageDifference))) AS ageDifference,
       CASE WHEN gender THEN 10 ELSE 0 END as sameGender

RETURN potentialFriend,
      {friendsInCommon: friendsInCommon,
       sameLocation: sameLocation,
       ageDifference:ageDifference,
       sameGender: sameGender} AS parts,
     friendsInCommon + sameLocation + ageDifference + sameGender AS score
ORDER BY score DESC

//9. The Daisy!
CREATE (a:Middle {name:""}) -[:STALK]-> (b:Root {name:""})
FOREACH (i in RANGE(1,10) | CREATE (a) -[:PETAL]-> (a))
RETURN a, b




//10. MERGE on dense nodes
// see http://stackoverflow.com/questions/30930311/how-can-i-optimise-a-neo4j-merge-query-on-a-node-with-many-relationships/30932777#30932777

convert this
MATCH (from:Node { id: 0 })
UNWIND RANGE(1,10000) AS i
MATCH (to:Node { id: i})
MERGE (to)<-[:HAS]-(from);

to

MATCH (from:Node { id: 0 })
UNWIND RANGE(1,100000) AS i
MATCH (to:Node { id: i})
WHERE shortestPath((to)<-[:HAS]-(from)) IS NULL
CREATE (from)-[:HAS]->(to);
