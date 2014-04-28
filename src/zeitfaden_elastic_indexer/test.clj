(ns com.zeitfaden.services.indexer
  
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



(def server1-conn {:pool {} :spec {:host "services.zeitfaden.com"}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(defmacro forever [& body]
  `(while true ~@body))





(defn connect-to-mongo []
  (mg/connect! { :host "services.zeitfaden.com"})
  (mg/set-db! (mg/get-db "zeitfaden_test"))
  )

(defn get-next-scheduled-station []
  (mc/find-one "indexer-schedule" {$or [{:shardId "handmade_shard_number_one"}
                                        {:shardId "handmade_shard_number_two"}]})
  )

(defn delete-scheduled-station [mongo-id]
  (mc/remove-by-id "indexer-schedule" mongo-id))









(defn connect-to-elastic []
  (esr/connect! "http://elastic-search.zeitfaden.com:9200")
  )

(defn create-station-index []
  (connect-to-elastic)
  (esi/create "clojure-stations" :settings {"number_of_shards" 1})
  )



(defn create-station-mapping []
  (connect-to-elastic)
  (let [mapping-types {"station" {:properties {:description {:type "string" :store "yes"}
                                               :title {:type "string" :store "yes"}
                                               :start_location {:type "geo_point"}
                                               :end_location {:type "geo_point"}

                                               }}}]
   
    (esi/update-mapping "clojure-stations" "station" :mapping mapping-types)))



(defn read-station-from-server [station-id]
  (let [url (str "http://test.zeitfaden.com/station/getById/stationId/" station-id)
        station-data (:body (http-client/get url {:decompress-body false}))]
    (json/read-str station-data)))


(defn write-station-to-index [station-id]
  (connect-to-elastic)
  (let [station-data (read-station-from-server station-id)]
    (println station-data)
    (esd/create "clojure-stations" "station" (assoc station-data :start_location {:lat (read-string (station-data "startLatitude")) :lon (read-string (station-data "startLongitude"))} :end_location {:lat (read-string (station-data "endLatitude")) :lon (read-string (station-data "endLongitude"))}) :id (station-data "id"))
    )

  )


(defn enrich-station-data [station-data]
  (assoc station-data :start_location {:lat (read-string (station-data "startLatitude")) :lon (read-string (station-data "startLongitude"))} :end_location {:lat (read-string (station-data "endLatitude")) :lon (read-string (station-data "endLongitude"))}))

  


(defn tobias []
  (println "some")
  ;(http-client/get "http://www.google.com")

  (wcar* (car/ping)
         (car/set "tobias" "yesyesyes")
         (car/get "tobias")
         (car/rpush "tobiliste", "werteins")
         ;(car/rpop "tobiliste")
         )


  (let [station-id (wcar* (car/lpop "tobiliste"))]
    (println "station id is " station-id)
    (http-client/get (str "http://www.tobiasgassmann.com?some=" station-id))))




(defn mongo-test []
  (let [mongo-station-object (get-next-scheduled-station)]
    (let [station-id (:stationId (from-db-object mongo-station-object  true))
          mongo-id (:_id (from-db-object mongo-station-object true))]
      (println "deleting from mongo")
      (delete-scheduled-station mongo-id)
      (println "writing to elastic")
      (write-station-to-index station-id)
      (println "done es"))))



(defn get-bulk-indexing-command [station-id]
  (let [indexing-command {"index" {"_index" "clojure-stations" "_type" "station" "_id" (str station-id)}}]
       (json/write-str indexing-command)))


(defn mongo-bulk-test []
  (let [mongo-station-object (get-next-scheduled-station)]
    (let [station-id (:stationId (from-db-object mongo-station-object  true))
          mongo-id (:_id (from-db-object mongo-station-object true))]
      (println "deleting from mongo")
      (delete-scheduled-station mongo-id)

      (let [conn (esr/connect! "http://elastic-search.zeitfaden.com:9200")]
        
        (let [station-data (read-station-from-server station-id)]
          (println "now making bulk-index")
          (let [bulk-content (eb/bulk-index (enrich-station-data station-data))]
            (println "done making the document")
            
            (println "now trying to bulk index")
            (let [generated-stuff (eb/bulk-index [{:_index "clojure-stations" :_type "station" :title "sometitle"} {:_index "clojure-stations" :_type "station" :title "some more title"}] ) ]
              (println "now the thing")
              (println generated-stuff)
              (println "now the json thing")
              (println (map json/write-str generated-stuff))
              (println "that was the thing")
              (eb/bulk generated-stuff :refresh true)))
          
          
          
          ))
      
      (println "not writing to elastic")
      
      (println "done es"))))



(defn mainsome []
  (connect-to-mongo)
  (forever (mongo-test)))



  











