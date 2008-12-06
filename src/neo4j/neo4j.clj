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

(defmacro with-neo [ #^String fname & body ]
  `(binding [*neo* (new ~EmbeddedNeo ~fname)]
    (try ~@body
      (finally (.shutdown *neo*)))))

(defmacro tx [& body]
  `(binding [*tx* (.beginTx *neo*)]
    (try ~@body
         (finally (.finish *tx*)))))