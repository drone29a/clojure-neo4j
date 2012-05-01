# A Neo4j library for Clojure #

The `clojure-neo4j` project provides a lispy interface to [Neo4j](http://neo4j.org), a graph-structured on-disk transactional database.

# Get It #

The latest version, [0.3.0-SNAPSHOT](http://clojars.org/clojure-neo4j/versions/0.3.0-SNAPSHOT), is available from clojars.

Add `[clojure-neo4j "0.3.0-SNAPSHOT"]` to your project's dependency list and fetch with `lein deps`.

# Building #

`lein jar`

# Example #

    (use '[neo4j.core :exclude [open shutdown]])

    (def db (neo4j.core/open "/path/to/db"))

    ;;; Create a root for customers and add a customer.
    (with-tx db
      (let [customer-root (new-node db) 
            bob (new-node db)]
        (relate (top-node db) :customers customer-root)
        (relate customer-root :customer bob)
        (properties bob {"name" "Bob"
                         "age" 30
                         "id" "C12345"})))

    ;;; Fetch all customer IDs
    (with-tx db
      (let [customer-root (-> (top-node db)
                              (.getSingleRelationship (relationship :customers) outgoing)
                              (.getEndNode))]
        (doall (map #(property % "id") 
                    (traverse customer-root
                              breadth-first
                              (depth-of 1)
                              all-but-start
                              {:customer outgoing})))))

    (neo4j.core/shutdown db)
