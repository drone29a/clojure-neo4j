(ns neo4j.core
  (:require [schema.core :as s])
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
           (org.neo4j.graphdb.index Index)
           (org.neo4j.graphdb DynamicLabel Label GraphDatabaseService)
           (org.neo4j.graphdb.schema IndexDefinition)
           (org.neo4j.kernel EmbeddedGraphDatabase)
           (org.neo4j.graphdb.factory GraphDatabaseFactory GraphDatabaseSettings GraphDatabaseBuilder)
           (org.neo4j.tooling GlobalGraphOperations)))

(declare properties!
         new-label)

(def Nil (s/pred nil?))
(def Key (s/either s/Str s/Keyword))
(def Val (s/either s/Str s/Bool s/Num))
(def Props {Key Val})

(defn open
  ([^String db-path]
   (-> (GraphDatabaseFactory.) (.newEmbeddedDatabase db-path)))
  ([^String db-path config]
   (let [builder (-> (GraphDatabaseFactory.) (.newEmbeddedDatabaseBuilder db-path))]
     (doseq [[k v] config]
       (.setConfig builder k v))
     (.newGraphDatabase builder))))

(defn shutdown!
  [^GraphDatabaseService db]
  (.shutdown db))

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
      (isStopNode [^TraversalPosition pos]
                  (== (.depth pos) d)))))
(def end-of-graph StopEvaluator/END_OF_GRAPH)

(def all ReturnableEvaluator/ALL)
(def all-but-start ReturnableEvaluator/ALL_BUT_START_NODE)

(defn success [^Transaction tx] (.success tx))

(defn failure [^Transaction tx] (.failure tx))

(defmacro with-tx [^GraphDatabaseService db & body]
  `(let [^Transaction tx# (.beginTx ^GraphDatabaseService ~db)]
     (try
       (let [val# (do ~@body)]
         (success tx#)
         val#)
       (finally (.finish tx#)))))

(defn name-or-str
  [x]
  (if (keyword? x) 
    (name x) 
    (str x)))

(s/defn new-node! :- Node
  ([db :- GraphDatabaseService]
     (.createNode db))
  ([db :- GraphDatabaseService 
    props :- {Key s/Any}]
     (let [node (new-node! db)]
       (properties! node props)
       node))
  ([db :- GraphDatabaseService
    label :- Key
    props :- Props]
     (doto (new-node! db props)
       (.addLabel (new-label (name-or-str label))))))

(defn delete-node! 
  "Delete the given node."
  [^Node n]
  (if-let [rs (.getRelationships n)]
    (doseq [^Relationship r rs]
      (.delete r)))
  (.delete n))

(s/defn relationship :- RelationshipType
  [n :- s/Keyword]
  (proxy [RelationshipType] []
    (name [] (name n))))

(s/defn relate! :- Relationship
  [type :- s/Keyword
   from :- Node
   to :- Node]
  (.createRelationshipTo from to (relationship type)))

(s/defn delete-relationship!
  [r :- Relationship]
  (.delete r))

(defn return-if [f]
  (proxy [ReturnableEvaluator] []
    (isReturnableNode [^TraversalPosition p] (f p))))

(defn stop-if [f]
  (proxy [StopEvaluator] []
    (isStopNode [^TraversalPosition p] (f p))))

(s/defn property :- (s/either Val Nil)
  "Return or set single property.
  Keys are always stored as strings and always returned as keywords."
  [c :- PropertyContainer
   key :- Key]
  (try
    (.getProperty c (name-or-str key))
    (catch org.neo4j.graphdb.NotFoundException e
      nil)))

(s/defn property! :- Nil
  [c :- PropertyContainer
   key :- Key
   val :- Val]
  (.setProperty c (name-or-str key) val))

(s/defn properties :- Props
  "Return or set a map of properties.
  Keys are always stored as strings and always returned as keywords."
  [c :- PropertyContainer]
  (let [ks (.getPropertyKeys c)]
    (into {} (map (fn [k] [(keyword k) (.getProperty c k)]) ks))))

(s/defn properties! :- Nil
  [c :- PropertyContainer
   props :- Props]
  (doseq [[k v] props]
    (.setProperty c (name-or-str k) v))
  nil)

(defn labels
  "Return labels of given node."
  [^Node n]
  (seq (.getLabels n)))

(s/defn new-label :- Label
  [name :- s/Str]
  (DynamicLabel/label name))

(s/defn new-index! :- IndexDefinition
  [db :- GraphDatabaseService
   label-name :- Key
   prop-name :- Key]
  (-> db
      (.schema)
      (.indexFor (-> label-name name-or-str new-label))
      (.on (name-or-str prop-name))
      (.create)))

(s/defn drop-index! :- (s/pred nil?)
  [idx :- Index]
  (.drop idx))

(defn traverse
  "Traverse the graph.  Starting at the given node, traverse the graph
  in either bread-first or depth-first order, stopping when the stop-fn returns
  true.  The filter-fn should return true for any node reached during the traversal
  that is to be returned in the sequence.  The map of relationships and directions
  is used to decide which edges to traverse."
  [^Node start-node order stop-evaluator return-evaluator relationship-direction]
  (.getAllNodes (.traverse start-node 
                           order
                           stop-evaluator
                           return-evaluator
                           (into-array Object (mapcat identity (map (fn [[k v]] 
                                                                      [(relationship k) v]) 
                                                                    relationship-direction))))))
;; TODO: need a better way to select individual node?
;;       this is kind of like query which can be filtered by single value...
(s/defn all-nodes :- [Node]
  ([db :- GraphDatabaseService]
   (seq (.getAllNodes (GlobalGraphOperations/at db))))
  ([db :- GraphDatabaseService 
    label-name :- Key]
   (seq (.getAllNodesWithLabel (GlobalGraphOperations/at db)
                                        (-> label-name name-or-str new-label))))
  ([db :- GraphDatabaseService
    label-name :- Key
    prop-name :- Key
    prop-val :- s/Any]
   (iterator-seq (.findNodes db
                             (-> label-name name-or-str new-label)
                             (name-or-str prop-name)
                             prop-val))))

(s/defn all-relationships
  [db :- GraphDatabaseService]
  (loop [] (mapcat (fn []))) (seq (.getAllRelationships (GlobalGraphOperations/at db))))

(comment
  
  (s/defn all-relationships
    [db :- GraphDatabaseService]
    (seq (.getAllRelationships (GlobalGraphOperations/at db))))
  
  (defn all-relationships
    [^GraphDatabaseService db]
    (let [step 100000]
      (loop [rels (transient [])
             pos 0]
        (let [next-rels (with-tx db
                          (->> (seq (.getAllRelationships (GlobalGraphOperations/at db)))
                               (drop pos)
                               (take step)
                               doall))]
          (println pos)
          (if (empty? next-rels)
            (persistent! rels)
            (recur (concat rels next-rels)
                   (+ pos step)))))))

  (defn all-relationships
    [^GraphDatabaseService db]
    (let [step 1000000]
      (lazy-seq (iterate (fn [s] (println (type s))
                           (if (empty? (with-tx db s))
                             nil
                             (with-tx db (->> s (take step) doall))))
                         (with-tx db (seq (.getAllRelationships (GlobalGraphOperations/at db)))))))))

(defn all-labels
  [^GraphDatabaseService db]
  (seq (.getAllLabels (GlobalGraphOperations/at db))))

(defn all-indexes
  ([^GraphDatabaseService db]
     (seq (-> db .schema .getIndexes)))
  ([^GraphDatabaseService db
    ^String label-name]
     (seq (-> db .schema (.getIndexes (new-label label-name))))))
