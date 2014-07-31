(defproject io.mandoline/mandoline-sqlite "0.1.1"
  :description "SQLite backend for Mandoline."
  :license {:name "Apache License, version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :min-lein-version "2.0.0"
  :url
    "https://github.com/TheClimateCorporation/mandoline-sqlite"
  :mailing-lists
    [{:name "mandoline-users@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-users"
      :post "mandoline-users@googlegroups.com"}
     {:name "mandoline-dev@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-dev"
      :post "mandoline-dev@googlegroups.com"}]

  :checksum :warn
  :dependencies
    [[org.clojure/clojure "1.5.1"]
     [org.slf4j/slf4j-log4j12 "1.7.2"]
     [log4j "1.2.17"]
     [org.clojure/tools.logging "0.2.6"]
     [org.clojure/java.jdbc "0.2.3"]
     [org.xerial/sqlite-jdbc "3.7.2"]
     [clj-dbcp "0.8.1"]
     [org.clojure/core.incubator "0.1.3"]
     [joda-time/joda-time "2.1"]
     [commons-io/commons-io "2.4"]
     [io.mandoline/mandoline-core "0.1.1"]]
  :exclusions [org.clojure/clojure]

  :profiles {
    :dev {:dependencies
           [[criterium "0.4.3"]]}}

  :aliases {"docs" ["marg" "-d" "target"]
            "package" ["do" "clean," "jar"]}

  :plugins [[lein-marginalia "0.7.1"]
            [lein-cloverage "1.0.2"]]
  :test-selectors {:default (complement :experimental)
                   :integration :integration
                   :experimental :experimental
                   :all (constantly true)})
