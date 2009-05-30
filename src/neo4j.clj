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

(defn name-or-str
  [x]
  (if (keyword? x) 
    (name x) 
    (str x)))

(defn new-node 
  ([] (.createNode *neo*))
  ([props] (let [node (new-node)]
             (set-properties node props)
             node)))

(defn top-node [] (.getReferenceNode *neo*))

(defn relationship [#^Keyword n]
  (proxy [RelationshipType] []
    (name [] (name n))))

(defn relate [#^Node from #^Keyword type #^Node to]
  (.createRelationshipTo from to (relationship type)))

(defn return-if [f]
  (proxy [ReturnableEvaluator] []
    (isReturnableNode [#^TraversalPosition p] (f p))))

(defn stop-if [f]
  (proxy [StopEvaluator] []
    (isStopNode [#^TraversalPosition p] (f p))))

(defn property
  [#^PropertyContainer c key]
  (.getProperty c (name key)))

(defn properties 
  "Return a map of properties."
  [#^PropertyContainer c]
  (let [ks (.getPropertyKeys c)]
    (into {} (map (fn [k] [(keyword k) (.getProperty c k)]) ks))))

(defn set-properties
  "Set properties of a node or relationship."
  [#^PropertyContainer c props]
  (doseq [[k v] props]
    (.setProperty c (name-or-str k) (or v "")))
  nil)

(defn node-delete 
  "Delete the given node."
  [#^Node n]
  (if-let [rs (.getRelationships n)]
    (doseq [r rs]
      (.delete r)))
  (.delete n))