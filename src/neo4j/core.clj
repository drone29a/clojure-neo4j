(ns neo4j
  (:import (org.neo4j.graphdb Direction
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
                              Traverser$Order)
	   (org.neo4j.kernel EmbeddedGraphDatabase)))

(declare properties)

(def *neo* nil)
(def *tx* nil)

(defn start
  [db-path]
  (alter-var-root #'*neo* (fn [_] (EmbeddedGraphDatabase. db-path))))

(defn shutdown
  []
  (.shutdown *neo*))

(def both Direction/BOTH)
(def incoming Direction/INCOMING)
(def outgoing Direction/OUTGOING)

(def breadth-first Traverser$Order/BREADTH_FIRST)
(def depth-first   Traverser$Order/DEPTH_FIRST)

(defn depth-of 
  "Return a StopEvaluator for the given traversal depth."
  [d] 
  (if (== d 1) 
    StopEvaluator/DEPTH_ONE
    (proxy [StopEvaluator] []
      (isStopNode [#^TraversalPosition pos]
                  (== (.depth pos) d)))))
(def end-of-graph StopEvaluator/END_OF_GRAPH)

(def all ReturnableEvaluator/ALL)
(def all-but-start ReturnableEvaluator/ALL_BUT_START_NODE)

(defmacro with-neo [#^String fname & body ]
  `(binding [*neo* (new ~EmbeddedGraphDatabase ~fname)]
    (try ~@body
      (finally (.shutdown *neo*)))))

(defmacro tx [& body]
  `(binding [*tx* (.beginTx *neo*)]
    (try ~@body
         (finally (.finish *tx*)))))

(defn success [] (.success *tx*))

(defn failure [] (.failure *tx*))

(defmacro with-tx [& body]
  `(tx
     (let [val# (do ~@body)]
       (success)
       val#)))

(defn name-or-str
  [x]
  (if (keyword? x) 
    (name x) 
    (str x)))

(defn new-node 
  ([] (.createNode *neo*))
  ([props] (let [node (new-node)]
             (properties node props)
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
  "Necessary?  Maybe remove it in favor of properties."
  ([#^PropertyContainer c key]
     (.getProperty c (name key)))
  ([#^PropertyContainer c key val]
     (.setProperty c (name-or-str key) val)))

(defn properties 
  "Return or set a map of properties."
  ([#^PropertyContainer c]
     (let [ks (.getPropertyKeys c)]
       (into {} (map (fn [k] [(keyword k) (.getProperty c k)]) ks))))
  ([#^PropertyContainer c props]
     (doseq [[k v] props]
       (.setProperty c (name-or-str k) (or v "")))
     nil))

(defn node-delete 
  "Delete the given node."
  [#^Node n]
  (if-let [rs (.getRelationships n)]
    (doseq [r rs]
      (.delete r)))
  (.delete n))

(defn traverse
  "Traverse the graph.  Starting at the given node, traverse the graph
in either bread-first or depth-first order, stopping when the stop-fn returns
true.  The filter-fn should return true for any node reached during the traversal
that is to be returned in the sequence.  The map of relationships and directions
is used to decide which edges to traverse."
  [#^Node start-node order stop-evaluator return-evaluator relationship-direction]
  (.getAllNodes (.traverse start-node 
                           order
                           stop-evaluator
                           return-evaluator
                           (into-array Object (mapcat identity (map (fn [[k v]] 
                                                                      [(relationship k) v]) 
                                                                    relationship-direction))))))
(defn lookup
  "Inside a transation, looks up nodes by index key for the given index."
  [idx & ids]
  (doall (map (fn [k] (.getSingleNodeFor idx k)) 
              ids)))