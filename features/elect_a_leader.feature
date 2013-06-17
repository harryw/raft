Feature: Elect a single leader for the cluster

  Scenario: Elect a leader when there is just one node
    Given there is a node on port 8000
    When I send the command "A" to the node on port 8000
    Then the node on port 8000 should be in the "LEADER" role

  Scenario: Elect a leader when there are many nodes
    Given there are nodes on the following ports:
      | 8000 |
      | 8001 |
      | 8002 |
      | 8003 |
      | 8004 |
    When I send the command "A" to the node on port 8000
    Then just one of the nodes should be in the "LEADER" role

  Scenario: Elect a leader from an empty state
    Given there are nodes on the following ports:
      | 8000 |
      | 8001 |
      | 8002 |
    And all the nodes have empty logs
    When I send the command "A" to the node on port 8000
    Then just one of the nodes should be in the "LEADER" role


  Scenario: Elect a node with a longer-than-most log
    Given there are nodes on the following ports:
      | 8000 |
      | 8001 |
      | 8002 |
    And the nodes on port 8000 has an empty log
    And the node on port 8001 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 1    | C       |
    And the node on port 8002 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
    And the node on port 8001's current term is 1
    When I send the command "D" to the node on port 8000
    Then a single node on one of the following ports should be in the "LEADER" role:
      | 8001 |
      | 8002 |


  Scenario: Accept a command and append it to the log
    Given there is a node on port 8000
    When I send the command "A" to the node on port 8000
    Then the node on port 8000 should have the following log:
      | index | term | command |
      | 0     | 1    | A       |

  Scenario: Replicate the leader's log to an empty log
    Given there are nodes on the following ports:
      | 8000 |
      | 8001 |
      | 8002 |
    And all the nodes have empty logs
    When I send the command "A" to the node on port 8000
    And I await full replication
    Then the node on port 8000 should have the following log:
      | index | term | command |
      | 0     | 1    | A       |

  Scenario: Replicate the leader's log to a conflicting log
    Given there are nodes on the following ports:
      | 8000 |
      | 8001 |
      | 8002 |
    And the node on port 8000 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 2    | C       |
    And the node on port 8001 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 2    | C       |
    And the node on port 8002 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 1    | D       |
    When I await full replication
    Then the node on port 8002 should have the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 2    | C       |

  Scenario: Don't let replication drop committed entries from the log
    Given there are nodes on the following ports:
      | 8000 |
      | 8001 |
      | 8002 |
    And the node on port 8000 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 2    | C       |
    And the node on port 8001 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 2    | C       |
    And the node on port 8002 has the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 1    | D       |
    And the node port port 8002 has as commit index of 2
    When I await full replication
    Then the node on port 8002 should have the following log:
      | index | term | command |
      | 0     | 0    | A       |
      | 1     | 1    | B       |
      | 2     | 2    | D       |


