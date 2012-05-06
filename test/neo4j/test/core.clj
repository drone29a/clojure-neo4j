(ns neo4j.test.core
  (:require [neo4j.core :as neo4j])
  (:use [clojure.test]))

(defn tmp-db-path [name]
  (format "/tmp/clojure-neo4j-tests-%s" name))

(deftest new-node
  (let [db (neo4j/open (tmp-db-path "new-nodes"))
        node (neo4j/with-tx db (neo4j/new-node db {"name" "foo"}))]
    (neo4j/with-tx db
      (is (= "foo" (neo4j/property (.getNodeById db (.getId node)) "name"))))
    (neo4j/shutdown db)))

(deftest relate
  (let [db (neo4j/open (tmp-db-path "relate"))
        src (neo4j/with-tx db (neo4j/new-node db {"name" "foo"}))
        dst (neo4j/with-tx db (neo4j/new-node db {"name" "bar"}))
        rel (neo4j/with-tx db (neo4j/relate src :likes dst))]
    (neo4j/with-tx db
      (is (= "likes" (-> (.getRelationshipById db (.getId rel))
                       (.getType)
                       (.name))))
      (is (= src (.getStartNode (.getRelationshipById db (.getId rel)))))
      (is (= dst (.getEndNode (.getRelationshipById db (.getId rel))))))
    (neo4j/shutdown db)))

(deftest traverse
  (let [db (neo4j/open (tmp-db-path "traverse"))
        user-root (neo4j/with-tx db (neo4j/new-node db {"name" "user-root"}))
        alice (neo4j/with-tx db (neo4j/new-node db {"name" "alice"}))
        bob (neo4j/with-tx db (neo4j/new-node db {"name" "bob"}))
        carol (neo4j/with-tx db (neo4j/new-node db {"name" "carol"}))]
    (neo4j/with-tx db
      (neo4j/relate (neo4j/top-node db) :users user-root)
      (neo4j/relate user-root :user alice)
      (neo4j/relate user-root :user bob)
      (neo4j/relate user-root :user carol)
      
      (neo4j/relate alice :likes bob)
      (neo4j/relate bob :likes carol)
      (neo4j/relate carol :likes alice))

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
  
    (neo4j/shutdown db)))

(deftest global-ops
  (let [db (neo4j/open (tmp-db-path "global-ops"))
        user-root (neo4j/with-tx db (neo4j/new-node db {"name" "user-root"}))
        alice (neo4j/with-tx db (neo4j/new-node db {"name" "alice"}))
        bob (neo4j/with-tx db (neo4j/new-node db {"name" "bob"}))
        carol (neo4j/with-tx db (neo4j/new-node db {"name" "carol"}))]
    (neo4j/with-tx db
      (neo4j/relate (neo4j/top-node db) :users user-root)
      (neo4j/relate user-root :user alice)
      (neo4j/relate user-root :user bob)
      (neo4j/relate user-root :user carol)
      
      (neo4j/relate alice :likes bob)
      (neo4j/relate bob :likes carol)
      (neo4j/relate carol :likes alice))

    (is (= 5 (count (seq (neo4j/all-nodes db)))))
    (is (= 7 (count (seq (neo4j/all-relationships db)))))
  
    (neo4j/shutdown db)))