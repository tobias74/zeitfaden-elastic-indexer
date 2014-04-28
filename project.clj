(defproject zeitfaden-elastic-indexer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
		 [com.taoensso/carmine "2.6.0"]
		 [org.clojure/data.json "0.2.4"]
		 [clojurewerkz/elastisch "2.0.0-beta5"]
		 [clj-http "0.9.1"]
		 [com.novemberain/monger "1.7.0"]
	        ]
  :main ^:skip-aot zeitfaden-elastic-indexer.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
