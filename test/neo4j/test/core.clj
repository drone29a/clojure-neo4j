(ns neo4j.test.core
  (:require [neo4j.core :as neo4j]
            [clojure.test :refer :all]
            [schema.test]))

(use-fixtures :once schema.test/validate-schemas)

(defn tmp-db-path [name]
  (format "/tmp/clojure-neo4j-tests-%s" name))

(deftest new-node
  (let [db (neo4j/open (tmp-db-path "new-nodes"))
        node (neo4j/with-tx db (neo4j/new-node! db {"name" "foo"}))
        labeled-node (neo4j/with-tx db (neo4j/new-node! db "le label" {"name" "bar"}))]
    (neo4j/with-tx db
      (is (= "foo" (neo4j/property (.getNodeById db (.getId node)) "name")))
      (is (= "bar" (neo4j/property (.getNodeById db (.getId labeled-node)) "name")))
      (is (contains? (set (neo4j/labels (.getNodeById db (.getId labeled-node)))) (neo4j/new-label "le label"))))
    (neo4j/shutdown! db)))

(deftest relate
  (let [db (neo4j/open (tmp-db-path "relate"))
        src (neo4j/with-tx db (neo4j/new-node! db {"name" "foo"}))
        dst (neo4j/with-tx db (neo4j/new-node! db {"name" "bar"}))
        rel (neo4j/with-tx db (neo4j/relate! :likes src dst))]
    (neo4j/with-tx db
      (is (= "likes" (-> (.getRelationshipById db (.getId rel))
                       (.getType)
                       (.name))))
      (is (= src (.getStartNode (.getRelationshipById db (.getId rel)))))
      (is (= dst (.getEndNode (.getRelationshipById db (.getId rel))))))
    (neo4j/shutdown! db)))

(deftest traverse
  (let [db (neo4j/open (tmp-db-path "traverse"))
        user-root (neo4j/with-tx db (neo4j/new-node! db {"name" "user-root"}))
        alice (neo4j/with-tx db (neo4j/new-node! db {"name" "alice"}))
        bob (neo4j/with-tx db (neo4j/new-node! db {"name" "bob"}))
        carol (neo4j/with-tx db (neo4j/new-node! db {"name" "carol"}))]
    (neo4j/with-tx db
      (neo4j/relate! :user user-root alice)
      (neo4j/relate! :user user-root bob)
      (neo4j/relate! :user user-root carol)
      
      (neo4j/relate! :likes alice bob)
      (neo4j/relate! :likes bob carol)
      (neo4j/relate! :likes carol alice))

    (neo4j/with-tx db
      (is (= 3 (count (neo4j/traverse user-root
                                      neo4j/breadth-first
                                      (neo4j/depth-of 1)
                                      neo4j/all-but-start
                                      {:user neo4j/outgoing})))))

    (neo4j/with-tx db
      (is (= #{"bob"} (set (doall (map #(neo4j/property % "name")
                                       (neo4j/traverse alice
                                                       neo4j/breadth-first
                                                       (neo4j/depth-of 1)
                                                       neo4j/all-but-start
                                                       {:likes neo4j/outgoing})))))))

    (neo4j/with-tx db
      (is (= #{"bob" "carol"} (set (doall (map #(neo4j/property % "name")
                                               (neo4j/traverse alice
                                                               neo4j/breadth-first
                                                               (neo4j/depth-of 2)
                                                               neo4j/all-but-start
                                                               {:likes neo4j/outgoing})))))))
  
    (neo4j/shutdown! db)))

(deftest global-ops
  (let [db (neo4j/open (tmp-db-path "global-ops"))
        user-root (neo4j/with-tx db (neo4j/new-node! db {"name" "user-root"}))
        alice (neo4j/with-tx db (neo4j/new-node! db {"name" "alice"}))
        bob (neo4j/with-tx db (neo4j/new-node! db {"name" "bob"}))
        carol (neo4j/with-tx db (neo4j/new-node! db {"name" "carol"}))]
    (neo4j/with-tx db
      (neo4j/relate! :user user-root alice)
      (neo4j/relate! :user user-root bob)
      (neo4j/relate! :user user-root carol)
      
      (neo4j/relate! :likes alice bob)
      (neo4j/relate! :likes bob carol)
      (neo4j/relate! :likes carol alice))

    (neo4j/with-tx db
      (is (= 4 (count (neo4j/all-nodes db))))
      (is (= 6 (count (neo4j/all-relationships db)))))
  
    (neo4j/shutdown! db)))

(deftest properties
  (let [db (neo4j/open (tmp-db-path "properties"))
        node (neo4j/with-tx db (neo4j/new-node! db {"name" "foo"}))]
    (neo4j/with-tx db
      (is (nil? (neo4j/property node "dne"))))
    (neo4j/shutdown! db)))

(deftest indexing
  (let [db (neo4j/open (tmp-db-path "indexing"))]
    (neo4j/with-tx db
      (neo4j/new-index! db "a-label" :a))
    (neo4j/with-tx db
      (neo4j/new-node! db "a-label" {:a "abc"
                                    :b 123})
      (neo4j/new-node! db "a-label" {:a "abc"
                                    :d 456})
      (neo4j/new-node! db "other-label" {:e "aiz"
                                        :f 136})
      (is (= 1 (count (neo4j/all-indexes db "a-label"))))
      (is (= 2 (count (neo4j/all-nodes db "a-label" :a "abc"))))
      (is (= #{{:a "abc"
                :b 123}
               {:a "abc"
                :d 456}}
             (->> (neo4j/all-nodes db "a-label" :a "abc")
                  (map neo4j/properties)
                  (set)))))))
