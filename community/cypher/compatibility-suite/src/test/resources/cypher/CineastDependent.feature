#
# Copyright (c) 2002-2017 "Neo Technology,"
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
Feature: CineastDependent

  Background:
    Given the cineast graph

  Scenario: should make query from existing database
    When executing query: MATCH (n) RETURN count(n)
    Then the result should be:
      | count(n) |
      | 63084    |

  Scenario: should support multiple divisions in aggregate function
    When executing query: MATCH (n) RETURN count(n)/60/60 as count
    Then the result should be:
      | count |
      | 17    |

  Scenario: should accept skip zero
    When executing query: MATCH (n) WHERE 1 = 0 RETURN n SKIP 0
    Then the result should be empty

  Scenario: should return collection size
    When executing query: return size([1,2,3]) as n
    Then the result should be:
      | n |
      | 3 |

  Scenario: should support column renaming for aggregates as well
    When executing query: MATCH (a) WHERE id(a) = 0 RETURN count(*) as ColumnName
    Then the result should be:
      | ColumnName |
      | 1          |

  Scenario: should be able to run coalesce
    When executing query: MATCH (a) WHERE id(a) = 0 RETURN coalesce(a.title, a.name)
    Then the result should be:
      | coalesce(a.title, a.name) |
      | 'Emil Eifrem'             |

  Scenario: should allow ordering on aggregate function
    When executing query: MATCH (n)-[:KNOWS]-(c) WHERE id(n) = 0 RETURN n, count(c) AS cnt ORDER BY cnt
    Then the result should be empty

  Scenario: should allow addition
    When executing query: MATCH (a) WHERE id(a) = 61263 RETURN a.version + 5
    Then the result should be:
      | a.version + 5 |
      | 1863          |

  Scenario: should allow absolute function
    When executing query: RETURN abs(-1)
    Then the result should be:
      | abs(-1) |
      | 1       |

  Scenario: functions should return null if they get path containing unbound
    When executing query: MATCH (a) WHERE id(a) = 1 OPTIONAL MATCH p=(a)-[r]->() RETURN length(nodes(p)), id(r), type(r), nodes(p), rels(p)
    Then the result should be:
      | length(nodes(p)) | id(r) | type(r) | nodes(p) | rels(p) |
      | null             | null  | null    | null     | null    |

  Scenario: aggregates inside normal functions should work
    When executing query: MATCH (a) RETURN length(collect(a))
    Then the result should be:
      | length(collect(a)) |
      | 63084              |
