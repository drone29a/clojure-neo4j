(ns neo4j
  (:import (org.neo4j.api.core Direction
                               EmbeddedNeo
                               NeoService
                               Node
                               NotFoundException
                               NotInTransactionException
                               PropertyContainer
                               Relationship
                               RelationshipType
                               ReturnableEvaluator
                               StopEvaluator
                               Transaction
                               TraversalPosition
                               Traverser
                               Traverser$Order)))

(def *neo* nil)
(def *tx* nil)

(def both Direction/BOTH)
(def incoming Direction/INCOMING)
(def outgoing Direction/OUTGOING)

(def breadth Traverser$Order/BREADTH_FIRST)
(def depth   Traverser$Order/DEPTH_FIRST)

(def depth-one StopEvaluator/DEPTH_ONE)
(def end-of-graph StopEvaluator/END_OF_GRAPH)

(def all ReturnableEvaluator/ALL)
(def all-but-start ReturnableEvaluator/ALL_BUT_START_NODE)

(defmacro with-neo [ #^String fname & body ]
  `(binding [*neo* (new ~EmbeddedNeo ~fname)]
    (try ~@body
      (finally (.shutdown *neo*)))))

(defmacro tx [& body]
  `(binding [*tx* (.beginTx *neo*)]
    (try ~@body
         (finally (.finish *tx*)))))

(defn success [] (.success *tx*))

(defn failure [] (.failure *tx*))

(defn new-node [] (.createNode *neo*))

(defn top-node [] (.getReferenceNode *neo*))

(defn relationship [#^Keyword n]
  (proxy [RelationshipType] []
    (name [] (name n))))

(defn relate [#^Node from #^Keyword type #^Node to]
  (.createRelationship from to (relationship type)))

(defn return-if [f]
  (proxy [ReturnableEvaluator] []
    (isReturnableNode [#^TraversalPosition p] (f p))))

(defn stop-if [f]
  (proxy [StopEvaluator] []
    (isStopNode [#^TraversalPosition p] (f p))))
