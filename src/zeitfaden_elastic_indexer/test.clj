(ns com.zeitfaden.services.indexer
  
  (:require [clj-http.client :as http-client]
            [taoensso.carmine :as car :refer (wcar)]
            [clojure.data.json :as json]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            ))



(def server1-conn {:pool {} :spec {:host "services.zeitfaden.com"}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))





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





(defn write-station-to-index [station-id]
  (connect-to-elastic)
  (let [station-data (read-station-from-server station-id)]
    (println station-data)
    (esd/create "clojure-stations" "station" (assoc station-data :start_location {:lat (read-string (station-data "startLatitude")) :lon (read-string (station-data "startLongitude"))} :end_location {:lat (read-string (station-data "endLatitude")) :lon (read-string (station-data "endLongitude"))}) :id (station-data "id"))
    )

  )

(defn read-station-from-server [station-id]
  (let [url (str "http://test.zeitfaden.com/station/getById/stationId/" station-id)
        station-data (:body (http-client/get url {:decompress-body false}))]
    (json/read-str station-data)))




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






  











