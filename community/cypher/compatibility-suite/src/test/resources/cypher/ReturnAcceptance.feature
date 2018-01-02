#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

@db:cineast
Feature: ReturnAcceptanceTest

  Scenario: should support multiple divisions in aggregate function
    Given using: cineast
    When running: MATCH (n) RETURN count(n)/60/60 as count;
    Then result:
      | count |
      | 17    |

  Scenario: should accept skip zero
    Given using: cineast
    When running: MATCH n WHERE 1 = 0 RETURN n SKIP 0;
    Then result:
      |  |

  Scenario: should limit to two hits
    Given init: CREATE ({name: "A"}), ({name: "B"}), ({name: "C"}), ({name: "D"}), ({name: "E"});
    When running: MATCH n RETURN n LIMIT 2;
    Then result:
      | n             |
      | ({name: "A"}) |
      | ({name: "B"}) |

  Scenario: should start the result from second row
    Given init: CREATE ({name: "A"}), ({name: "B"}), ({name: "C"}), ({name: "D"}), ({name: "E"});
    When running: MATCH n RETURN n ORDER BY n.name ASC SKIP 2;
    Then result:
      | n             |
      | ({name: "C"}) |
      | ({name: "D"}) |
      | ({name: "E"}) |

  Scenario: should start the result from second row by param
    Given init: CREATE ({name: "A"}), ({name: "B"}), ({name: "C"}), ({name: "D"}), ({name: "E"});
    When running parametrized: MATCH n RETURN n ORDER BY n.name ASC SKIP { skipAmount };
      | skipAmount |
      | 2          |
    Then result:
      | n             |
      | ({name: "C"}) |
      | ({name: "D"}) |
      | ({name: "E"}) |

  Scenario: should get stuff in the middle
    Given init: CREATE ({name: "A"}), ({name: "B"}), ({name: "C"}), ({name: "D"}), ({name: "E"});
    When running: MATCH (n) WHERE id(n) IN [0,1,2,3,4] RETURN n ORDER BY n.name ASC SKIP 2 LIMIT 2;
    Then result:
      | n             |
      | ({name: "C"}) |
      | ({name: "D"}) |

  Scenario: should get stuff in the middle by param
    Given init: CREATE ({name: "A"}), ({name: "B"}), ({name: "C"}), ({name: "D"}), ({name: "E"});
    When running parametrized: MATCH (n) WHERE id(n) IN [0,1,2,3,4] RETURN n ORDER BY n.name ASC SKIP { s } LIMIT { l };
      | s | l |
      | 2 | 2 |
    Then result:
      | n             |
      | ({name: "C"}) |
      | ({name: "D"}) |

  Scenario: should sort on aggregated function
    Given init: CREATE ({division: "A", age: 22}), ({division: "B", age: 33}), ({division: "B", age: 44}), ({division: "C", age: 55});
    When running: MATCH n WHERE id(n) IN [0,1,2,3] RETURN n.division, max(n.age) ORDER BY max(n.age);
    Then sorted result:
      | n.division | max(n.age) |
      | A          | 22         |
      | B          | 44         |
      | C          | 55         |

  Scenario: should return collection size
    Given using: cineast
    When running: return size([1,2,3]) as n;
    Then result:
      | n |
      | 3 |

  Scenario: should support sort and distinct
    Given init: CREATE ({name: "A"}), ({name: "B"}), ({name: "C"});
    When running: MATCH a WHERE id(a) IN [0,1,2,0] RETURN DISTINCT a ORDER BY a.name;
    Then result:
      | a             |
      | ({name: "A"}) |
      | ({name: "B"}) |
      | ({name: "C"}) |

  Scenario: should support column renaming
    Given init: CREATE (:Singleton);
    When running: MATCH (a) WHERE id(a) = 0 RETURN a as ColumnName;
    Then result:
      | ColumnName   |
      | (:Singleton) |

  Scenario: should support column renaming for aggregates as well
    Given using: cineast
    When running: MATCH (a) WHERE id(a) = 0 RETURN count(*) as ColumnName;
    Then result:
      | ColumnName |
      | 1          |

  Scenario: should support ordering by a property after being distinctified
    Given init: CREATE (:A)-[:T]->(:B);
    When running: MATCH a-->b WHERE id(a) = 0 RETURN DISTINCT b ORDER BY b.name;
    Then result:
      | b    |
      | (:B) |

  Scenario: should be able to run coalesce
    Given using: cineast
    When running: MATCH (a) WHERE id(a) = 0 RETURN coalesce(a.title, a.name);
    Then result:
      | coalesce(a.title, a.name) |
      | Emil Eifrem               |

  Scenario: should allow ordering on aggregate function
    Given using: cineast
    When running: MATCH (n)-[:KNOWS]-(c) WHERE id(n) = 0 RETURN n, count(c) AS cnt ORDER BY cnt;
    Then result:
      |  |

  Scenario: arithmetic precedence test
    Given using: cineast
    When running: RETURN 12 / 4 * 3 - 2 * 4;
    Then result:
      | 12 / 4 * 3 - 2 * 4 |
      | 1                  |

  Scenario: arithmetic precedence with parenthesis test
    Given using: cineast
    When running: RETURN 12 / 4 * (3 - 2 * 4);
    Then result:
      | 12 / 4 * (3 - 2 * 4) |
      | -15                  |

  Scenario: should allow addition
    Given using: cineast
    When running: MATCH (a) WHERE id(a) = 61263 RETURN a.version + 5;
    Then result:
      | a.version + 5 |
      | 1863          |

  Scenario: should allow absolute function
    Given using: cineast
    When running: RETURN abs(-1);
    Then result:
      | abs(-1) |
      | 1.0     |

  Scenario: count star should count everything in scope
    Given init: CREATE (:l1), (:l2), (:l3);
    When running: MATCH (a) RETURN a, count(*) ORDER BY count(*);
    Then result:
      | a     | count(*) |
      | (:l1) | 1        |
      | (:l2) | 1        |
      | (:l3) | 1        |

  Scenario: functions should return null if they get path containing unbound
    Given using: cineast
    When running: MATCH (a) WHERE id(a) = 1 OPTIONAL MATCH p=a-[r]->() RETURN length(nodes(p)), id(r), type(r), nodes(p), rels(p);
    Then result:
      | length(nodes(p)) | id(r) | type(r) | nodes(p) | rels(p) |
      | null             | null  | null    | null     | null    |

  Scenario: aggregates inside normal functions should work
    Given using: cineast
    When running: MATCH a RETURN length(collect(a));
    Then result:
      | length(collect(a)) |
      | 63084              |

  Scenario: filter should work
    Given init: CREATE (a { foo: 1 })-[:T]->({ foo: 1 }), (a)-[:T]->({ foo: 2 }), (a)-[:T]->({ foo: 3 });
    When running: MATCH (a { foo: 1 }) MATCH p=a-->() RETURN filter(x IN nodes(p) WHERE x.foo > 2) AS n;
    Then result:
      | n            |
      | [({foo: 3})] |
      | []           |
      | []           |
