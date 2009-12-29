(defproject clojure-neo4j "0.1.0-SNAPSHOT" 
  :description "The clojure-neo4j project provides a more lispy interface to Neo4j, a graph-structured on-disk transactional database." 
  :dependencies [[org.clojure/clojure "1.1.0-master-SNAPSHOT"] 
                [org.clojure/clojure-contrib "1.0-SNAPSHOT"]]
  :repositories [["org.neo4j" "http://m2.neo4j.org"]]
  :dev-dependencies   [[org.neo4j/neo "1.0-b11"]
                      [org.neo4j/index-util "0.9"]])