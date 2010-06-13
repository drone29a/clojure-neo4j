(defproject clojure-neo4j "0.2.0" 
  :description "The clojure-neo4j project provides a more lispy interface to Neo4j, a graph-structured on-disk transactional database." 
  :dependencies [[org.clojure/clojure "1.1.0-master-SNAPSHOT"] 
                [org.clojure/clojure-contrib "1.0-SNAPSHOT"]
                [org.neo4j/neo4j-kernel "1.0"]
                [org.neo4j/neo4j-index "1.0"]]
  :repositories [["org.neo4j" "http://m2.neo4j.org"]]
  :dev-dependencies   [[lein-clojars "0.5.0-SNAPSHOT"]
		       [swank-clojure "1.1.0"]
		       [leiningen/lein-swank "1.1.0"]])