(ns zeitfaden-elastic-indexer.core
  (:gen-class)
  (:require 
   [clojure.tools.cli :refer [parse-opts]]
   [zeitfaden-elastic-indexer.test :refer [bulksome]]

   ))


(def cli-options
  ;; An option with a required argument
  [["-t" "--target TARGETNAME" "Target Name"
    :default "test"
    
    ]
   ["-w" "--worker WORKERNAME" "Worker Name"
    :default "worker-one"

    ]
   ["-l" "--loops TOTALLOOPSE" "Total Loopse"
    :default 10
    :parse-fn #(Integer/parseInt %)

    ]
   ["-s" "--size BATCHSIZE" "Batch Size"
    :default 200
    :parse-fn #(Integer/parseInt %)

    ]
   ;; A non-idempotent option
   ["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   ;; A boolean option defaulting to nil
      ["-h" "--help"]])


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  (let [my-args (parse-opts args cli-options)]
    

    (let [my-options (:options my-args)]
      (println my-options)
      (bulksome
       (:target my-options)
       (:worker my-options)
       (:loops my-options)
       (:size my-options)))

    
    
    )
  (println "done"))
