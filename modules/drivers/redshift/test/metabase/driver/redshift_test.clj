(ns metabase.driver.redshift-test
  (:require [clojure.test :refer :all]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.plugins.jdbc-proxy :as jdbc-proxy]
            [metabase.test :as mt]
            [metabase.test
             [fixtures :as fixtures]
             [util :as tu]]
            [metabase.test.data.datasets :refer [expect-with-driver]]))

(use-fixtures :once (fixtures/initialize :plugins))

(expect-with-driver :redshift
  "UTC"
  (tu/db-timezone-id))

(deftest correct-driver-test
  (is (= "com.amazon.redshift.jdbc.Driver"
         (.getName (class (jdbc-proxy/wrapped-driver (java.sql.DriverManager/getDriver "jdbc:redshift://host:5432/testdb")))))
      "Make sure we're using the correct driver for Redshift"))

(deftest default-read-column-thunk-test
  (mt/test-driver :redshift
    (testing "The Redshift driver should use the same default read-column-thunk implementation as Postgres"
      (is (= (get-method sql-jdbc.execute/read-column-thunk :postgres)
             (get-method sql-jdbc.execute/read-column-thunk :redshift))))))
