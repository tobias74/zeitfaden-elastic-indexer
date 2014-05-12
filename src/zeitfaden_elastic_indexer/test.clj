(ns zeitfaden-elastic-indexer.test
  
      
  (:require [clj-http.client :as http-client]
            [taoensso.carmine :as car :refer (wcar)]
            [clojure.data.json :as json]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.bulk :as eb]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.conversion :refer [from-db-object]]
            [monger.operators :refer :all]
           
            )

  (:import [com.mongodb MongoOptions ServerAddress]
           [org.bson.types ObjectId]))





(def system-config (atom nil))
(def worker-name (atom nil))
(def worker-shard (atom nil))





(def server1-conn {:pool {} :spec {:host "services.zeitfaden.com"}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(defmacro forever [& body]
  `(while true ~@body))




(def test-system-config
  {:mongo-host "services.zeitfaden.com"
   :mongo-database "zeitfaden_test"
   :mongo-scheduler-collection "indexer-schedule"
   :station-index-name "clojure-stations-test"
   :zf-api-url "test.zeitfaden.com"
   })


(def live-system-config
  {:mongo-host "services.zeitfaden.com"
   :mongo-database "zeitfaden_live"
   :mongo-scheduler-collection "indexer-schedule"
   :station-index-name "clojure-stations-live"
   :zf-api-url "livetest.zeitfaden.com"
   })


(defn connect-to-mongo []
  (mg/connect! { :host (:mongo-host @system-config)})
  (mg/set-db! (mg/get-db (:mongo-database @system-config))))


(defn get-next-scheduled-stations [station-counter]
  (take station-counter
        (mc/find-maps
         (:mongo-scheduler-collection @system-config)
         {$and [{:shardId @worker-shard}
               {:loadBalanceWorker @worker-name}]}))
  )



(defn delete-scheduled-station [mongo-id]
  (mc/remove-by-id (:mongo-scheduler-collection @system-config) mongo-id))



(defn connect-to-elastic []
  (esr/connect! "http://elastic-search.zeitfaden.com:9200")
  )


(defn create-station-index []
  (connect-to-elastic)
  (esi/create (:station-index-name @system-config) :settings {"number_of_shards" 1}))


(defn create-station-mapping []
  (connect-to-elastic)
  (let [mapping-types {"station" {:properties {:description {:type "string" :store "yes"}
                                               :title {:type "string" :store "yes"}
                                               :startDateWithId {:type "string" :index "not_analyzed"}
                                               :startLocation {:type "geo_point"}
                                               :endLocation {:type "geo_point"}}}}]
   
    (esi/update-mapping (:station-index-name @system-config) "station" :mapping mapping-types)))



(defn enrich-station-data [station-data]
  (assoc station-data
    :_id (station-data "id")
    :startDateWithId (str (station-data "startDate") "_" (station-data "id"))
    :startLocation {:lat (read-string (station-data "startLatitude")) :lon (read-string (station-data "startLongitude"))}
    :endLocation {:lat (read-string (station-data "endLatitude")) :lon (read-string (station-data "endLongitude"))}))

  


(defn get-bulk-indexing-command [station-id]
  (let [indexing-command {"index" {"_index" (:station-index-name @system-config) "_type" "station" "_id" (str station-id)}}]
       (json/write-str indexing-command)))


(defn build-query-string [station-ids]
  (clojure.string/join "&" (map #(str "stationIds[]=" %) station-ids)))


(defn read-stations-from-server [station-ids]
  (let [url (str "http://" (:zf-api-url @system-config) "/station/getByIds/")
        station-data (:body (http-client/post url {:decompress-body false :form-params {:stationIds (json/write-str station-ids)}}))]
    (json/read-str station-data)))


(defn digest-next-scheduled-stations-data [station-counter]
  (let [data-hashes (get-next-scheduled-stations station-counter)]
    (doseq [x data-hashes]
      (let [station-id (:stationId x)
            mongo-id (:_id x)]
        (delete-scheduled-station mongo-id)))

    (let [station-ids (map #(:stationId %) data-hashes)]
      (let [stations (map enrich-station-data (read-stations-from-server station-ids))]
        (let [generated-stuff (eb/bulk-index stations ) ]
          (eb/bulk-with-index-and-type (:station-index-name @system-config) "station" generated-stuff :refresh true))))))





(defn bulksome [target my-worker-shard my-worker-name total-loops batch-size]
  (if (= target "live")
    (reset! system-config live-system-config)
    (reset! system-config test-system-config))
  (reset! worker-name my-worker-name)
  (reset! worker-shard my-worker-shard)
  (connect-to-mongo)
  (connect-to-elastic)

  (try 
    (create-station-index)
    (catch Exception e (println "Creating Index failed, possibly it already exists?")))
  
  (try 
    (create-station-mapping)
    (catch Exception e (println "Creating mapping failed?")))

  (println "inside th ebulkmsome" target my-worker-name total-loops batch-size)
  (dorun total-loops (repeatedly #(digest-next-scheduled-stations-data batch-size)))
  (println "done"))

  








