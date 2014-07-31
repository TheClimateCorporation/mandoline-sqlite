(ns io.mandoline.backend.sqlite
  "A Mandoline store implementation that uses SQLite databases that are
  persisted on the local file system.

  This store persists each dataset as a separate SQLite database on the
  local filesystem. Each per-dataset SQLite database has 3 tables:

  - \"chunks\": This table stores content-addressable binary chunks of
    the dataset. The columns are \"chunk-id\" (TEXT),
    \"reference-count\" (INTEGER), \"data\" (BLOB). All versions of the
    dataset share the same \"chunks\" table. The `SQLiteChunkStore` type
    interacts with this table.
  - \"indices\": This table stores mappings from coordinates within a
    versioned dataset to chunk. The columns are \"version-id\" (TEXT),
    \"coordinates\" (TEXT), \"chunk-id\" (TEXT). The \"coordinates\"
    column contains a composite value that includes the variable name
    and chunk coordinates within the variable. All versions of the
    dataset share the same \"indices\" table. The `SQLiteIndex` type
    interacts with this table.
  - \"versions\": This table stores metadata for the version history of
    the dataset. The columns are \"version-id\" (TEXT), \"timestamp\"
    (TEXT, ISO-8601 encoded), \"metadata\" (TEXT, JSON encoded). The
    `SQLiteConnection` type interacts with this table.

  The `mk-schema` function in this namespace instantiates a
  `SQLiteSchema` type with a `root-path` argument that represents a
  parent directory. Every dataset that belongs to this schema instance
  has a sub-directory under this parent directory. For example, the
  dataset \"foo\" would be stored in the directory

      <root-path>/foo/

  Each dataset directory contains the SQLite database and journal files
  for that dataset. Together, these files store the dataset in a durable
  and portable format. A dataset directory can be safely moved to a new
  path and opened with a new schema (using the root of the destination
  path) and a new dataset name (the basename of the destination path).

  The SQLite store uses SQLite database transactions to ensure
  consistency at the expense of performance. Multiple threads and
  multiple processes can safely read and write the same dataset;
  however, performance will suffer from exponential backoff and retry
  upon database contention.

  WARNING: Because this store relies on atomic operations of the local
  filesystem, consistency is not guaranteed when using a network
  filesystem."
  (:require
    [clojure.core.strint :refer [<<]]
    [clojure.java.io :refer [file]]
    [clojure.java.jdbc :as jdbc]
    [clojure.string :refer [join]]
    [clojure.tools.logging :as log]
    [clj-dbcp.core :as dbcp]
    [io.mandoline.impl.protocol :as proto]
    [io.mandoline.utils :as utils])
  (:import
    [java.io File IOException]
    [java.nio ByteBuffer]
    [java.sql SQLException]
    [org.apache.commons.io FileUtils]
    [org.joda.time DateTime]))

(defn- exponential-backoff
  "Sleep for a random time interval that is generated according to the
  exponential backoff formula.

  `tick` is slot time in milliseconds, and `attempt` is the backoff
  round. See http://en.wikipedia.org/wiki/Exponential_backoff "
  [tick attempt]
  (let [sleep (long (* tick (rand-int (bit-shift-left 1 attempt))))]
    (Thread/sleep sleep)))

(defn- exponential-backoff-and-retry
  [thunk {tick :sleep tries :tries catch-fn :catch :as options}]
  (letfn [(try-fn [attempt]
            (try
              [(thunk) true]
              (catch Exception e
                (when-not (catch-fn e)
                  (throw e))
                (when-not (< attempt tries)
                  (log/warnf
                    e
                    "Exception caught after %d tries; no more retries."
                    tries)
                  (throw e))
                (log/debugf
                  e
                  "Exception caught on %dth of %d tries; retrying."
                  attempt tries)
                (exponential-backoff tick attempt)
                [nil false])))]
    (loop [attempt 0]
      (let [[return success?] (try-fn attempt)]
        (if success?
          return
          (recur (inc attempt)))))))

(def ^:dynamic *default-retry-options*
  {:sleep 5 ; 5 ms
   :tries 10 ; up to 10 attempts
   ; Catch SQLException instance when the message matches
   ; "database is locked".
   :catch (fn [e]
            (and
              (instance? java.sql.SQLException e)
              (let [m (.getMessage e)]
                (some #(re-seq % m) [#"database is locked"]))))})

(defmacro retry-with-db
  "This macro is similar to the `clojure.java.jdbc/with-connection`
  macro, except that it sets the journaling mode to write-ahead logging
  (WAL) before evaluating the body, and it retries on exception.

  The behavior of this macro can be customized by binding the dynamic
  var *default-retry-options*.

  Performance is significantly faster with WAL than without. See
  documentation at http://www.sqlite.org/wal.html"
  [db-spec options & body]
  `(let [options# (merge *default-retry-options* ~options)
         thunk# (fn [] (jdbc/with-connection ~db-spec
                         (-> (jdbc/connection)
                           (jdbc/prepare-statement "PRAGMA journal_mode=WAL")
                           (.execute))
                         ~@body))]
     (exponential-backoff-and-retry thunk# options#)))

(defmacro with-query-results
  "Execute a parametrized PreparedStatement query, then evaluate an
  expression on the results that were returned by this query.

  This macro takes the following arguments:

  `query-results`
      A symbol that is bound to a seq of result maps (as returned by the
      `clojure.java.jdbc/resultseq-seq` function). Depending on the
      query, the seq may be empty. The seq of result maps is eagerly
      realized within the transaction; callers ought to be mindful of
      performance when there are many query results.
  `sql`
      SQL prepared statement template (string).
  `param-group`
      Collection of parameter values to substitute into the statement.
      The number of elements in the collection must match the number of
      \"?\" placeholders in the `sql` template. If the template does not
      contain any \"?\" placeholders, then `param-group` must be empty.
  `body`
      Variable number of forms to evaluate with the `query-results`
      symbol binding.

  Example:

  (with-query-results query-results
    \"SELECT z FROM table WHERE x=? AND y=?\" [\"a\" 1]
    (when-not (seq query-results)
      (println \"Query selected zero records!\"))
    (map :z query-results))

  This macro is intended to be nested within the retry-with-db macro."
  [query-results sql param-group & body]
  `(let [r# (gensym)]
     (jdbc/with-query-results r#
       (vec (cons ~sql ~param-group))
       ; Eagerly realize the seq. The underlying ResultSet is not
       ; immutable/persistent, so lazy access can trigger
       ; NullPointerException.
       (let [~query-results (doall r#)] ~@body))))

(defmacro with-no-more-than-one-query-result
  "This macro is similar to the with-query-results macro, except that it
  applies further processing to the results seq:

  - It asserts that the query returns no more than 1 result.
  - If the query returns a non-empty result, then the body is evaluated
    within binding to that (first and only) result.
  - If the query returns zero results, then nil is returned."
  [query-result sql param-group & body]
  `(let [r# (gensym)
         query-results# (with-query-results r# ~sql ~param-group r#)
         f# (fn [~query-result] ~@body)]
     (assert (<= (count query-results#) 1))
     (when (seq query-results#)
       (f# (first query-results#)))))

(defmacro with-transaction-update-count
  "Execute a parametrized PreparedStatement in a transaction, then
  evaluate an expression on the count of records that were updated in
  this transaction.

  This macro takes the following arguments:

  `update-count`
      A symbol that is bound to an integer, which is the number of
      records that were updated by the transaction. Depending on the
      transaction, the update count may be zero.
  `sql`
      SQL prepared statement template (string).
  `param-group`
      Collection of parameter values to substitute into the statement.
      The number of elements in the collection must match the number of
      \"?\" placeholders in the `sql` template. If the template does not
      contain any \"?\" placeholders, then `param-group` must be empty.
  `body`
      Variable number of forms to evaluate with the `update-count`
      symbol binding.

  Example:

  (with-transaction-update-count c
    \"INSERT INTO table(x,y,z) VALUES (?,?,?)\" [\"a\" 1 2]
    (when-not (= 1 c)
      (println \"Number of updated records does not equal one!\"))
    {:count c})

  This macro is intended to be nested within the retry-with-db macro."
  [update-count sql param-group & body]
  `(let [update-counts# (jdbc/do-prepared ~sql ~param-group)
         f# (fn [~update-count] ~@body)]
     (f# (first update-counts#))))

(defmacro interpolate-sql-identifiers
  "Given template string(s), interpolate a double-quoted SQL identifier
  at each position where a ~{} delimited tag appears in the template.

  Example:

  user=> (interpolate-sql-identifiers \"SELECT ~{x},~{y} FROM ~{table}\"
  \"SELECT \\\"x\\\",\\\"y\\\" FROM \\\"table\\\"
  "
  [& strings]
  (let [template (apply str strings)
        tags (map symbol (re-seq #"(?<=~\{)[^\{\}]*(?=\})" template))
        values (map (partial jdbc/as-quoted-identifier \") tags)
        bindings (vec (interleave tags values))]
    `(let ~bindings (<< ~template))))

(defn sqlite-connection-pool
  "Given a java.io.File argument that represents a SQLite database file,
  return a `clojure.java.jdbc`-style spec map for a database connection
  pool."
  [file]
  (let [ds (dbcp/make-datasource
             {:classname 'org.sqlite.JDBC
              :jdbc-url (format "jdbc:sqlite://%s" (.getCanonicalPath file))
              ; The SQLite that is included with org.xerial/sqlite-jdbc
              ; JAR is compiled so that it sleeps in *whole seconds*
              ; ticks when there is contention. Because contention
              ; causes such a long wait, performance is better when the
              ; connection pool is only a single thread.
              ;
              ; - http://beets.radbox.org/blog/sqlite-nightmare.html
              ; - https://bitbucket.org/xerial/sqlite-jdbc/issue/129/sqlite-sleeps-for-whole-second-1000-ms
              :max-active 1})]
    {:datasource ds}))

(defn- ^bytes byte-buffer->byte-array!
  "Given a java.nio.ByteBuffer instance, return a byte array whose
  contents are read from the ByteBuffer.

  This function consumes bytes starting from the current position of the
  ByteBuffer all the way to its limit. The position of the ByteBuffer is
  mutated as a side effect.

  This function is not thread-safe."
  [^java.nio.ByteBuffer byte-buffer]
  (let [ba (byte-array (.remaining byte-buffer))] ; read from current position
    (.get byte-buffer ba)
    (.rewind byte-buffer)
    ba))

(defn- canonicalize-coordinates
  "Given dataset coordinates, return a canonical representation in the
  format

      <var-name>|<coords 0>/<coords 1>/<coords 2>/...

  For example, \"x|2/3\" is the canonical representation of chunk [2 3]
  of the \"x\" variable."
  [var-name chunk-coords]
  (join "|" [var-name (join "/" chunk-coords)]))

(defn- version-id-as-string-hack
  "TODO: Remove this hack when version identifiers are consistently
  represented as string type."
  [x]
  (log/debug "TODO: remove this hack!")
  (cond
    (string? x) x
    (number? x) (str x)))

(defn- version-id-as-long-hack
  "TODO: Remove this hack when version identifiers are consistently
  represented as string type."
  [x]
  (log/debug "TODO: remove this hack!")
  (cond
    (integer? x) (long x)
    (string? x) (Long/parseLong x)))

(defn- database-size
  "Use PRAGMA queries to infer the storage size of the database.

  This function is intended to be called within the retry-with-db macro."
  []
  (let [page-size (with-no-more-than-one-query-result r
                    "PRAGMA page_size" []
                    (:page_size r))
        page-count (with-no-more-than-one-query-result r
                     "PRAGMA page_count" []
                     (:page_count r))]
    (* page-size page-count)))

(defn- count-records
  "Count the number of records in a table.

  This function is intended to be called within the retry-with-db macro."
  [table]
  (let [sql (format
              (interpolate-sql-identifiers
                "SELECT Count(*) AS ~{count} FROM %s")
              (jdbc/as-quoted-identifier \" table))
        params []] ; no parameters
    (with-no-more-than-one-query-result r sql params
      ; Count(*) returns a single result.
      (:count r))))

(def ^:private table-schemas
  "Private data that is used by the create-mandoline-tables! function."
  {:chunks   [[:chunk-id "TEXT" "NOT NULL" "PRIMARY KEY"]
              [:reference-count "INTEGER" "NOT NULL"]
              [:data "BLOB" "NOT NULL"]]
   :indices  [[:version-id "TEXT" "NOT NULL"] ; string, not number!
              ; The "coordinates" field is a formatted string that is
              ; generated by the canonicalize-coordinates function. The
              ; format is
              ;
              ;     <var-name><pipe(|)><slash(/)-delimited coordinates>
              ;
              ; e.g. "x|2/3" represents chunk [2 3] of the "x" variable.
              [:coordinates "TEXT" "NOT NULL"]
              [:chunk-id "TEXT" "NOT NULL" (interpolate-sql-identifiers
                                             "REFERENCES ~{chunks}")]
              ; Setting this primary key ensures that conflicting
              ; records that share the ("version-id","coordinates") but
              ; point to different chunks can never be inserted.
              [(interpolate-sql-identifiers
                 "PRIMARY KEY (~{version-id},~{coordinates}) "
                 "ON CONFLICT ROLLBACK")]]
   :versions [[:version-id "TEXT" "NOT NULL" "PRIMARY KEY"] ; string, not number!
              [:timestamp "TEXT" "UNIQUE"] ; ISO8601 string
              [:metadata "TEXT"]]})

(def ^:private timestamp-trigger
  "Private data that is used by the create-mandoline-tables! function."
  (interpolate-sql-identifiers
    "CREATE TRIGGER ~{set-version-timestamp} "
    "AFTER INSERT ON ~{versions} "
    "BEGIN UPDATE ~{versions} "
    "SET ~{timestamp}=strftime('%Y-%m-%dT%H:%M:%SZ', 'now') "
    "WHERE rowid=NEW.rowid; END;"))

(defn- create-mandoline-tables!
  "Create empty tables for chunks, indices, and versions.

  The versions table is created with a trigger that automatically sets
  the timestamp field in each record. The tables are created in a
  transaction that is rolled back if any of the CREATE TABLE statements
  failed.

  This function is intended to be called within the retry-with-db macro."
  []
  (let [commands (concat
                   ; CREATE TABLE statements
                   (for [[table-name schema] table-schemas]
                     (jdbc/with-quoted-identifiers \"
                       (apply jdbc/create-table-ddl table-name schema)))
                   ; CREATE TRIGGER statement
                   [timestamp-trigger])]
    (apply jdbc/do-commands commands))
  ; Verify that all tables exist.
  (let [conn (jdbc/find-connection)
        tables (set (map #(keyword (:table_name %))
                         (-> (.getMetaData conn)
                           (.getTables nil nil nil nil)
                           (jdbc/resultset-seq))))]
    (assert (every? tables (keys table-schemas)))))

(defn- is-directory?
  [dir]
  (let [dir* (file dir)]
    (and (.exists dir*) (.isDirectory dir*))))

(defn- rand-pos-long
  "Generate a random positive long."
  []
  (long (* (rand) Long/MAX_VALUE)))

(defn- create-temp-dir
  "Create an empty directory with a unique name under a parent
  directory."
  ; This function is similar to the createTempFile method of
  ; java.io.File, except that it creates a directory instead of a file.
  [prefix directory]
  (loop [counter 0]
    (let [base-name (str prefix (rand-pos-long))
          f (file directory base-name)]
      (cond
        (>= counter 10000) (throw
                             (IOException.
                               "Failed to create temporary directory"))
        ; mkdir returns false when directory creation fails
        (.mkdir f) f
        :else (recur (inc counter))))))

(defn- move-dir!
  "Move (rename) a directory from a source path to a destination path.

  If a directory already exists at the destination, this function does
  nothing and returns false. If the move operation succeeds, this
  function returns true. Otherwise, this function throws an exception.

  WARNING: Atomicity is not guaranteed! The exact behavior of this
  function depends on the host operating system."
  [^java.io.File src ^java.io.File dest]
  ; This implementation tries to solve 2 consistency problems:
  ;   1. Guarantee that, if a directory already exists at dest, it is
  ;      not overwritten.
  ;   2. Guarantee that the directory at src is atomically moved to
  ;      dest.
  ;
  ; To solve (1), the java.io.File/mkdir method is used to test whether
  ; a directory already exists at dest and create an empty directory at
  ; dest if it is available.
  ;
  ; To solve (2), the java.io.File/renameTo method is used to move src
  ; to the reserved dest. Unfortunately there is no atomicity guarantee!
  ; According to Java documentation, "Many aspects of the behavior of
  ; this method are inherently platform-dependent". renameTo is atomic
  ; in Linux when src and dest are on the same mounted filesystem.
  (if (.mkdir dest)
    ; The mkdir method
    ; - returns true if and only if the directory was created (which
    ;   implies that the directory did not already exist)
    ; - returns false otherwise
    (try
      (or
        ; The renameTo method
        ; - returns true if and only if the renaming succeeded
        ; - returns false otherwise
        (.renameTo src dest)
        ; To avoid a stale lock, delete the empty directory before
        ; returning false.
        (do (.delete dest) false))
      (catch Exception e
        ; To avoid a stale lock, delete the empty directory before
        ; propagating exception.
        (.delete dest)
        (log/error e e)
        (throw e)))
    false))

(defn- create-dataset!
  "Given a root path string and a Mandoline dataset name, create SQLite
  database and journal files, create tables for the dataset's chunks,
  indices, and versions, and return nil.

  If `root-path` is \"/mand/oline/path/\" and the dataset name is
  \"foo\", then this function will create a new directory at the path
  \"/mand/oline/path/foo/\" that contains SQLite database file(s). This
  function will throw an exception if the directory already exists.

  This function is thread-safe if the underlying filesystem has an atomic
  rename operation."
  [root-path name retry-options]
  (when-not (is-directory? root-path)
    (throw
      (IllegalArgumentException.
        (format
          "%s does not correspond to an accessible directory"
          root-path))))
  ; This function performs the following steps:
  ;   1. Create a hidden temporary directory under the root directory,
  ;      e.g. <root-path>/.creating-XYZ/ Do not use the default
  ;      java.io.tmpdir directory, because it may not not be on the same
  ;      mounted filesystem as the destination. The list-datasets
  ;      function must avoid listing this hidden directory.
  ;   2. Create SQLite database and journal file(s) in the temporary
  ;      hidden directory. The database is first created in the hidden
  ;      directory so that nobody tries to access it before it is ready.
  ;   3. After the database has been created, rename the hidden
  ;      directory <root-path>/.creating-XYZ/ to the public path
  ;      <root-path>/<dataset-name>. We want the rename to be atomic so
  ;      that the database is never in a partial state.
  (let [dir (file root-path name)
        temp-dir (create-temp-dir ".creating-" root-path)]
    (try
      (when (.isHidden dir)
        (throw
          (IllegalArgumentException.
            (format "%s is not a valid dataset name" name))))
      (when (.exists dir)
        (throw
          (IOException.
            (format "Dataset directory %s already exists" dir))))
      (let [f (file temp-dir "sqlite.db")
            db-spec (sqlite-connection-pool f)]
        (retry-with-db db-spec retry-options
          (create-mandoline-tables!)))
      ; move-dir! may return true or false. If it returns false,
      ; then assume that another thread or process created the
      ; database during the intervening time, and throw an
      ; exception.
      (or
        (move-dir! temp-dir dir)
        (throw
          (IOException.
            (format
              "Dataset directory %s already exists or can not be created"
              dir))))
      ; If temp-dir still exists, clean it up. Use deleteQuietly to
      ; avoid throwing an exception if other threads/processes race to
      ; delete the same directory.
      (finally (FileUtils/deleteQuietly temp-dir))))
    ; Return nil
    nil)

(defn- destroy-dataset!
  "Given a root path string and a Mandoline dataset name, delete the
  directory that contains the corresponding SQLite database and journal
  files and return nil.

  If `root-path` is \"/mand/oline/path/\" and the dataset name is
  \"foo\", then this function will delete the directory at
  \"/mand/oline/path/foo/\" and all of its contents, if this directory
  exists.

  This function is idempotent and has no effect if the database
  directory does not exist."
  [root-path name]
  (when-not (is-directory? root-path)
    (throw
      (IllegalArgumentException.
        (format
          "%s does not correspond to an accessible directory"
          root-path))))
  (let [dir (file root-path name)
        ; temp-dir is expected to not exist yet
        temp-dir (file root-path (str ".destroying-" (rand-pos-long)))]
    (try
      ; Rename the directory to the hidden directory. After the move,
      ; directory contents can be safely deleted. move-dir! may return
      ; false if a concurrent destroy-dataset! call already moved the
      ; directory.
      (move-dir! dir temp-dir)
      ; If the directory still exists at the original path, then
      ; throw an exception.
      (when (is-directory? dir)
        (throw
          (IOException.
            (format
              "Failed to move dataset directory %s to %s for deletion"
              dir temp-dir))))
      ; If temp-dir still exists, clean it up. Use deleteQuietly to
      ; avoid throwing an exception if other threads/processes race to
      ; delete the same directory.
      (finally (FileUtils/deleteQuietly temp-dir)))))

(defn- list-datasets
  "Given a root path string or java.io.File, return a set of strings
  that are the names of Mandoline datasets under this root path.

  This function provides an eventually consistent view of available
  datasets; recent creation or deletion operations may not be
  reflected."
  [root-path]
  (let [dir (file root-path)]
    (when-not (is-directory? dir)
      (throw
        (IllegalArgumentException.
          (format "%s is not a directory that exists." dir))))
    (filter
      ; Each non-hidden directory directly under root-path corresponds
      ; to a dataset
      #(let [f (file dir %)] (and (.isDirectory f) (not (.isHidden f))))
      (.list dir))))


(deftype SQLiteIndex [db options var-name metadata]
  proto/Index

  (target [_]
    {:metadata metadata :var-name var-name})

  (chunk-at [_ coordinates]
    (when coordinates
      ; This query retrieves records whose version-id field satisfies
      ; the following conditions:
      ;   1. The version-id appears in the versions table - i.e., it is
      ;      a commited version.
      ;   2. The version-id is less than or equal to the version-id in
      ;      the metadata map of this index instance. This condition
      ;      ensures that "future" versions are not accidentally leaked.
      (let [sql (interpolate-sql-identifiers
                  "SELECT ~{chunk-id} FROM ~{indices} "
                  "WHERE ~{version-id}<=? AND ~{coordinates}=? "
                  "AND EXISTS (SELECT 1 FROM ~{versions} "
                  "WHERE ~{versions}.~{version-id}=~{indices}.~{version-id}) "
                  "ORDER BY ~{version-id} DESC LIMIT 1")
            params [(version-id-as-string-hack (:version-id metadata))
                    (canonicalize-coordinates var-name coordinates)]]
        (retry-with-db db options
          (with-no-more-than-one-query-result r sql params
            (:chunk-id r))))))

  (chunk-at [this coordinates version-id]
    (when (empty? (version-id-as-string-hack version-id))
      (throw
        (IllegalArgumentException. "version-id must not be empty")))
    ; If the version-id in the index metadata is earlier than the
    ; requested version-id, return nil. The index ought not leak a
    ; "future" version.
    (when (and
            coordinates
            (>= 0 (compare
                    (version-id-as-string-hack version-id)
                    (version-id-as-string-hack (:version-id metadata)))))
      ; This query retrieves a record whose version-id field
      ; matches the requested version-id. This version-id does not
      ; need to appear in the versions table - i.e., it can be an
      ; uncommitted version.
      (let [sql (interpolate-sql-identifiers
                  "SELECT ~{chunk-id} FROM ~{indices} "
                  "WHERE ~{version-id}=? AND ~{coordinates}=? "
                  "LIMIT 1")
            params [(version-id-as-string-hack version-id)
                    (canonicalize-coordinates var-name coordinates)]]
        (retry-with-db db options
          (with-no-more-than-one-query-result r sql params
            (:chunk-id r))))))

  (write-index [_ coordinates old-hash new-hash]
    (if old-hash
      ; Change the chunk-id of an existing record, only when the current
      ; chunk-id equals an expected hash. This UPDATE is like a
      ; conditional put in DynamoDB.
      (let [sql (interpolate-sql-identifiers
                  "UPDATE ~{indices} SET ~{chunk-id}=? "
                  "WHERE ~{version-id}=? AND ~{coordinates}=? "
                  "AND ~{chunk-id}=?")
            params [new-hash
                    (version-id-as-string-hack (:version-id metadata))
                    (canonicalize-coordinates var-name coordinates)
                    old-hash]]
        (retry-with-db db options
          (with-transaction-update-count c sql params
            (assert (<= c 1))
            (pos? c)))) ; Return true if a record was updated.

      ; This INSERT will have no effect when it conflicts with the
      ; primary key ("version-id","coordinates") of an existing record.
      (let [sql (interpolate-sql-identifiers
                  "INSERT OR IGNORE INTO "
                  "~{indices}(~{version-id},~{coordinates},~{chunk-id}) "
                  "VALUES (?,?,?)")
            params [(version-id-as-string-hack (:version-id metadata))
                    (canonicalize-coordinates var-name coordinates)
                    new-hash]]
        (retry-with-db db options
          (with-transaction-update-count c sql params
            (assert (<= c 1))
            (pos? c)))))) ; Return true if a record was updated.

  (flush-index [_]
    ; The write-index method is eager and atomic, so flush-index is no-op.
    nil))


(deftype SQLiteChunkStore [db options]
  proto/ChunkStore

  (read-chunk [_ hash]
    (let [sql (interpolate-sql-identifiers
                "SELECT ~{data} FROM ~{chunks} WHERE ~{chunk-id}=?")
          params [hash]
          bb (retry-with-db db options
               (with-no-more-than-one-query-result r sql params
                 ; According to the JDBC specification, we ought to
                 ; get a java.sql.Blob instance from the ResultSet.
                 ; However, org.xerial/sqlite-jdbc is non-compliant
                 ; and returns byte[] instead.
                 (ByteBuffer/wrap (:data r))))]
      (when-not bb
        (throw
          (IllegalArgumentException.
            (format "No chunk was found for hash %s" hash))))
      bb))

  (chunk-refs [_ hash]
    (let [sql (interpolate-sql-identifiers
                "SELECT ~{reference-count} FROM ~{chunks} "
                "WHERE ~{chunk-id}=?")
          params [hash]
          ref-count (retry-with-db db options
                      (with-no-more-than-one-query-result r sql params
                        (long (:reference-count r))))]
      (when-not ref-count
        (throw
          (IllegalArgumentException.
            (format "No chunk was found for hash %s" hash))))
      ref-count))

  (write-chunk [_ hash ref-count bytes]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? ref-count)
      (throw
        (IllegalArgumentException. "ref-count must be an integer")))
    (when-not (instance? ByteBuffer bytes)
      (throw
        (IllegalArgumentException. "bytes must be a ByteBuffer instance")))
    (when-not (pos? (.remaining bytes))
      (throw
        (IllegalArgumentException. "Chunk has no remaining bytes")))
    (log/debugf "Writing chunk at hash: %s" hash)
    ; If INSERT would violate a UNIQUE constraint, then REPLACE the
    ; conflicting record.
    ; - http://www.sqlite.org/lang_insert.html
    ; - http://www.sqlite.org/lang_conflict.html
    (let [sql (interpolate-sql-identifiers
                "INSERT OR REPLACE INTO "
                "~{chunks}(~{chunk-id},~{reference-count},~{data}) "
                "VALUES (?,?,?)")
          ; According to the JDBC specification, we ought to provide a
          ; java.sql.Blob instance in the PreparedStatement. However,
          ; org.xerial/sqlite-jdbc is non-compliant and expects byte[]
          ; instead.
          ; Calling the rewind() method on the byte buffer is a
          ; necessary precaution because it sometimes arrives at the end
          ; position, which would cause an empty byte array to be
          ; written to the chunk store!
          params [hash
                  ref-count
                  (byte-buffer->byte-array! (.rewind bytes))]]
      (retry-with-db db options
        (with-transaction-update-count _ sql params
         nil)))
    (log/debugf "Finished writing chunk at hash: %s" hash)) ; Return nil

  (update-chunk-refs [_ hash delta]
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? delta)
      (throw
        (IllegalArgumentException. "delta must be an integer")))
    (log/debugf
      "Adding %d to reference count for chunk with hash: %s" delta hash)
    (let [sql (interpolate-sql-identifiers
                "UPDATE ~{chunks} "
                "SET ~{reference-count}=~{reference-count}+? "
                "WHERE ~{chunk-id}=?")
          params [delta hash]]
      (retry-with-db db options
        (with-transaction-update-count _ sql params
          nil))))) ; Return nil


(deftype SQLiteConnection [db-spec options]
  proto/Connection

  (index [_ var-name metadata _]
    (->SQLiteIndex db-spec options var-name metadata))

  (chunk-store [_ _]
    (->SQLiteChunkStore db-spec options))

  (write-version [_ metadata]
    ; The :versions table has a UNIQUE constraint on its :version-id
    ; column. It also has a TRIGGER to update the :timestamp column
    ; when a record is inserted.
    (let [sql (interpolate-sql-identifiers
                "INSERT INTO ~{versions}(~{version-id},~{metadata}) "
                "VALUES (?,?)")
          params [(version-id-as-string-hack (:version-id metadata))
                  (utils/generate-metadata metadata)]]
      (retry-with-db db-spec options
        (with-transaction-update-count _ sql params
          nil)))) ; Return nil

  (get-stats [_]
    (retry-with-db db-spec options
      {:metadata-count (count-records :versions)
       :index-count (count-records :indices)
       :data-count (count-records :chunks)
       :total-size (database-size)}))

  (metadata [_ version]
    (let [sql (interpolate-sql-identifiers
                "SELECT ~{metadata} FROM ~{versions} "
                "WHERE ~{version-id}=?")
          params [(version-id-as-string-hack version)]]
      (retry-with-db db-spec options
        (with-no-more-than-one-query-result r sql params
          (when (nil? r)
            (throw
              (RuntimeException.
                (format "version %s was not found" version))))
          (utils/parse-metadata (:metadata r) true)))))

  (versions [_ {:keys [limit metadata?]}]
    (let [sql (interpolate-sql-identifiers
                "SELECT * FROM ~{versions} "
                "ORDER BY ~{version-id} DESC LIMIT ?")
          ; https://www.sqlite.org/lang_select.html#limitoffset
          ; If the LIMIT expression evaluates to a negative value, then
          ; there is no upper bound on the number of rows returned.
          params [(if limit limit -1)]]
      (retry-with-db db-spec options
        (with-query-results rs sql params
          (for [r rs]
            (merge
              {:timestamp (DateTime. (:timestamp r))
               :version (version-id-as-long-hack (:version-id r))}
              (when metadata?
                {:metadata (utils/parse-metadata (:metadata r) true)}))))))))


(deftype SQLiteSchema [root-path options]
  proto/Schema

  (create-dataset [_ name]
    (create-dataset! root-path name options))

  (destroy-dataset [_ name]
    (destroy-dataset! root-path name))

  (list-datasets [_]
    (list-datasets root-path))

  (connect [_ name]
    (let [; Each dataset has its own directory, <root-dir>/<name>/
          dataset-dir (file root-path name)
          db-spec (sqlite-connection-pool (file dataset-dir "sqlite.db"))
          conn (->SQLiteConnection db-spec options)]
      (try ; Check that the connection is functional
        (.get-stats conn)
        (catch Exception e
          (throw
            (RuntimeException.
              (format
                "Failed to connect to dataset \"%s\" with root-path \"%s\""
                name root-path)
              e))))
      conn)))


(defn mk-schema
  "Instantiate a SQLiteSchema instance from a DynamoDB store spec."
  ([store-spec]
   (mk-schema store-spec {})) ; use default options
  ([store-spec options]
   (let [root-path (if (:db-version store-spec)
                     (join "/" (:root store-spec) (:db-version store-spec))
                     (:root store-spec))]
     (->SQLiteSchema root-path options))))
