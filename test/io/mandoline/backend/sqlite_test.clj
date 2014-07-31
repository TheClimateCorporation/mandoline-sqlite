(ns io.mandoline.backend.sqlite-test
  (:require
    [clojure.test :refer :all]
    [criterium.core :refer [benchmark
                            report-result]]
    [io.mandoline.backend.sqlite :as sqlite]
    [io.mandoline.chunk :refer [generate-id]]
    [io.mandoline.impl :refer [mk-schema]]
    [io.mandoline.test.utils :refer :all]
    [io.mandoline.test
     [concurrency :refer [lots-of-overlaps
                          lots-of-processes
                          lots-of-tiny-slices]]
     [entire-flow :refer [entire-flow]]
     [failed-ingest :refer [failed-write]]
     [grow :refer [grow-dataset]]
     [linear-versions :refer [linear-versions]]
     [nan :refer [fill-double
                  fill-float
                  fill-short]]
     [overwrite :refer [overwrite-dataset
                        overwrite-extend-dataset]]
     [scalar :refer [write-scalar]]
     [shrink :refer [shrink-dataset]]]
    [io.mandoline.test.protocol
     [chunk-store :as chunk-store]
     [schema :as schema]])
  (:import
    [java.io File]
    [java.nio ByteBuffer]
    [java.util UUID]
    [org.apache.commons.codec.digest DigestUtils]
    [org.apache.commons.io FileUtils]
    [com.google.common.io Files]))

(defn- setup
  "Create a random store spec for testing the SQLite Mandoline backend.

  This function is intended to be used with the matching teardown
  function."
  []
  (let [root (.getCanonicalPath (Files/createTempDir))
        dataset (str (UUID/randomUUID))]
    {:store "io.mandoline.backend.sqlite/mk-schema"
     :root root
     :dataset dataset}))

(defn- teardown
  "Given a store spec for testing the SQLite Mandoline backend,
  destructively clean up test data.

  This function is intended to be used with the matching setup
  function."
  [store-spec]
  (FileUtils/deleteQuietly (File. (:root store-spec))))

(deftest ^:integration test-sqlite-chunk-store-properties
  (let [root (atom nil)
        setup-chunk-store (fn []
                            (reset! root (Files/createTempDir))
                            (-> (doto (sqlite/->SQLiteSchema @root {})
                                  (.create-dataset "test"))
                              (.connect "test")
                              (.chunk-store nil)))
        teardown-chunk-store (fn [_]
                               (FileUtils/deleteQuietly @root)
                               (reset! root nil))
        num-chunks 100]
    (chunk-store/test-chunk-store-properties-single-threaded
      setup-chunk-store teardown-chunk-store num-chunks)
    (chunk-store/test-chunk-store-properties-multi-threaded
      setup-chunk-store teardown-chunk-store num-chunks)))

(deftest ^:integration test-sqlite-schema-properties
  (let [store-specs (atom {}) ; map of Schema instance -> store-spec
        setup-schema (fn []
                       (let [store-spec (setup)
                             s (sqlite/mk-schema store-spec)]
                         (swap! store-specs assoc s store-spec)
                         s))
        teardown-schema (fn [s]
                          (let [store-spec (@store-specs s)]
                            (teardown store-spec)))
        num-datasets 10]
    (schema/test-schema-properties-single-threaded
      setup-schema teardown-schema num-datasets)
    (schema/test-schema-properties-multi-threaded
      setup-schema teardown-schema num-datasets)))

(deftest ^:integration sqlite-entire-flow
  (with-and-without-caches
    (entire-flow setup teardown)))

(deftest ^:integration sqlite-grow-dataset
  (with-and-without-caches
    (grow-dataset setup teardown)))

(deftest ^:integration sqlite-shrink-dataset
  (with-and-without-caches
    (shrink-dataset setup teardown)))

(deftest ^:integration sqlite-overwrite-dataset
  (with-and-without-caches
    (overwrite-dataset setup teardown)))

(deftest ^:integration sqlite-overwrite-extend-dataset
  (with-and-without-caches
    (overwrite-extend-dataset setup teardown)))

(deftest ^:integration sqlite-linear-versions
  (with-and-without-caches
    (linear-versions setup teardown)))

(deftest ^:integration sqlite-write-scalar
  (with-and-without-caches
    (write-scalar setup teardown)))

(deftest ^:integration sqlite-lots-of-processes-ordered
  (lots-of-processes setup teardown false))

(deftest ^:integration sqlite-lots-of-processes-misordered
  (lots-of-processes setup teardown true))

(deftest ^:integration sqlite-lots-of-tiny-slices
  (with-and-without-caches
    (lots-of-tiny-slices setup teardown)))

(deftest ^:integration sqlite-write-fail-write
  (with-and-without-caches
    (failed-write setup teardown)))

(deftest ^:integration sqlite-lots-of-overlaps
  (with-and-without-caches
    (lots-of-overlaps setup teardown)))

(deftest ^:integration sqlite-nan-fill-values
  (with-and-without-caches
    (fill-double setup teardown)
    (fill-float setup teardown)
    (fill-short setup teardown)))

(deftest ^:experimental sqlite-chunk-store-write-benchmark
  ; WARNING: Running this test sometimes triggeres SIGSEGV
  ; # Problematic frame
  ; # C  [sqlite-3.7.2-libsqlitejdbc.so+0xe242]  short+0xbb
  (with-test-out
    (testing "Benchmarking concurrent writes to SQLiteChunkStore"
      (println (testing-contexts-str))
      (with-temp-db store-spec setup teardown
        (let [chunk-store (-> (mk-schema store-spec)
                            (.connect (:dataset store-spec))
                            (.chunk-store {}))
              random (java.util.Random.)
              chunks (doall
                       (for [i (range 100)
                             :let [bytes (ByteBuffer/wrap
                                           (let [ba (byte-array 64000)]
                                             (.nextBytes random ba)
                                             ba))]]
                         {:bytes bytes
                          :hash (DigestUtils/shaHex (.array bytes))
                          :ref-count (rand-int 10)}))
              write! (fn [{:keys [hash ref-count bytes]}]
                       (.write-chunk chunk-store hash ref-count bytes))
              statistics (benchmark
                           (doall (pmap write! chunks))
                           nil)
              upper-quantile (first (:upper-q statistics))
              threshold 1.0] ; threshold in seconds
          (is (< upper-quantile threshold)
              (format
                "%f quantile execution time ought to be < %f seconds"
                (- 1.0 (:tail-quantile statistics))
                threshold))
          (report-result statistics))))))
