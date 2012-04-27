(ns neo4j.test.core
  (:require [neo4j.core :as neo4j])
  (:use [clojure.test]))

(def tmp-db-path "/tmp/clojure-neo4j-tests")

(deftest new-node
  (let [db (neo4j/open tmp-db-path)
        node (neo4j/with-tx db (neo4j/new-node db {"name" "foo"}))]
    (neo4j/with-tx db
      (is (= "foo" (neo4j/property (.getNodeById db (.getId node)) "name"))))
    (neo4j/shutdown db)))

(deftest relate
  (let [db (neo4j/open tmp-db-path)
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
  (let [db (neo4j/open tmp-db-path)
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
