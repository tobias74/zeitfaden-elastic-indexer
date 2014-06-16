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
(def user-data-hash (atom {}))




(def server1-conn {:pool {} :spec {:host "services.zeitfaden.com"}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(defmacro forever [& body]
  `(while true ~@body))




(def test-system-config
  {:mongo-host "services.zeitfaden.com"
   :mongo-database "zeitfaden_test"
   :mongo-scheduler-collection "indexer-schedule"
   :mongo-scheduler-user-collection "indexer-schedule-users"
   :station-index-name "clojure-stations-test"
   :station-anonymous-index-name "clojure-stations-anonymous-test"
   :zf-api-url "test.zeitfaden.com"
   })


(def live-system-config
  {:mongo-host "services.zeitfaden.com"
   :mongo-database "zeitfaden_live"
   :mongo-scheduler-collection "indexer-schedule"
   :mongo-scheduler-user-collection "indexer-schedule-users"
   :station-index-name "clojure-stations-live"
   :station-anonymous-index-name "clojure-stations-anonymous-live"
   :zf-api-url "livetest.zeitfaden.com"
   })


(defn connect-to-mongo []
  (mg/connect! { :host (:mongo-host @system-config)})
  (mg/set-db! (mg/get-db (:mongo-database @system-config))))


(defn get-next-scheduled-stations [station-counter]
  (take station-counter
        (mc/find-maps
         (:mongo-scheduler-collection @system-config)
        ; {$and [{:shardId @worker-shard} {:loadBalanceWorker @worker-name}]}
         )))

(defn get-next-scheduled-users [user-counter]
  (take user-counter
        (mc/find-maps
         (:mongo-scheduler-user-collection @system-config)
        ; {$and [{:shardId @worker-shard} {:loadBalanceWorker @worker-name}]}
         )))


(defn delete-scheduled-station [mongo-id]
  (mc/remove-by-id (:mongo-scheduler-collection @system-config) mongo-id))


(defn delete-scheduled-user [mongo-id]
  (mc/remove-by-id (:mongo-scheduler-user-collection @system-config) mongo-id))



(defn connect-to-elastic []
  (esr/connect! "http://elastic-search.zeitfaden.com:9200")
  )


(defn create-station-index []
  (esi/create (:station-index-name @system-config) :settings {"number_of_shards" 1}))

(defn create-station-anonymous-index []
  (esi/create (:station-anonymous-index-name @system-config) :settings {"number_of_shards" 1}))

(def station-mapping {:description {:type "string" :store "yes"}
                      :title {:type "string" :store "yes"}
                      :startDateWithId {:type "string" :index "not_analyzed"}
                      :fileType {:type "string" :index "not_analyzed"}
                      :startLocation {:type "geo_point"}
                      :endLocation {:type "geo_point"}})

(defn create-station-mapping []
  (let [mapping-types {"station" {:_parent {:type "user"}
                                  :properties station-mapping}}]
    (esi/update-mapping (:station-index-name @system-config) "station" :mapping mapping-types)))


(defn create-station-anonymous-mapping []
  (let [mapping-types {"station" {:properties station-mapping}}]
    (esi/update-mapping (:station-anonymous-index-name @system-config) "station" :mapping mapping-types)))


(defn create-user-mapping []
  (let [mapping-types {"user" {:properties {:nickname {:type "string" :store "yes"}
                                            :userId {:type "string" :store "yes"}
                                            :fileType {:type "string" :index "not_analyzed"}}}}]
    (esi/update-mapping (:station-index-name @system-config) "user" :mapping mapping-types)))



(defn read-users-from-server [user-ids]
  (let [url (str "http://" (:zf-api-url @system-config) "/user/getByIds/loadBalancedUrls/0/")
        user-data (:body (http-client/post url {:decompress-body false :form-params {:userIds (json/write-str user-ids)}}))]
    (json/read-str user-data)))


(defn get-user-data [user-id]
  (if (contains? @user-data-hash user-id)
    (get @user-data-hash user-id)
    (do
      (println (str "inserting new user-id into our hash-map" user-id))
      
      (let [url (str "http://" (:zf-api-url @system-config) (str  "/user/getById/userId/" user-id "/loadBalancedUrls/0"))
            user-data (json/read-str (:body (http-client/get url {:decompress-body false})))]

        (let [new-user-data-hash (assoc @user-data-hash user-id user-data)]
          (reset! user-data-hash new-user-data-hash)
          (get @user-data-hash user-id))))))



(defn enrich-station-data [station-data]
  (let [intermediate-data 
        (assoc station-data
          :_id (station-data "id")
          :startDateWithId (str (station-data "startDate") "_" (station-data "id"))
          :startLocation {:lat (read-string (station-data "startLatitude")) :lon (read-string (station-data "startLongitude"))}
          :endLocation {:lat (read-string (station-data "endLatitude")) :lon (read-string (station-data "endLongitude"))})]
    
    (if (or  (clojure.string/blank? (station-data "userId")) (false? (station-data "userId")) (= "false" (station-data "userId")) )
      (assoc intermediate-data :_index (:station-anonymous-index-name @system-config) :_type "station")
      (do
        (let [user-data (get-user-data (station-data "userId"))]
          (assoc intermediate-data
            :_index (:station-index-name @system-config)
            :_type "station"
            :_parent (station-data "userId")
            :userSmallFrontImageUrl (user-data "smallFrontImageUrl")
            :userMediumFrontImageUrl (user-data "mediumFrontImageUrl")
            :userBigFrontImageUrl (user-data "bigFrontImageUrl")
            :userFileType (user-data "fileType")))))))



(defn enrich-user-data [user-data]
  (let [intermediate-data 
        (assoc user-data
          :_id (user-data "id"))]
    (assoc intermediate-data :_index (:station-index-name @system-config) :_type "user" )))
  


(defn read-stations-from-server [station-ids]
  (let [url (str "http://" (:zf-api-url @system-config) "/station/getByIds/loadBalancedUrls/0/")
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
          (eb/bulk generated-stuff :refresh true))))))



(defn digest-next-scheduled-users-data [user-counter]
  (let [data-hashes (get-next-scheduled-users user-counter)]
    (doseq [x data-hashes]
      (let [user-id (:userId x)
            mongo-id (:_id x)]
        (delete-scheduled-user mongo-id)))

    (let [user-ids (map #(:userId %) data-hashes)]
      (let [users (map enrich-user-data (read-users-from-server user-ids))]
        (let [generated-stuff (eb/bulk-index users ) ]
          (eb/bulk generated-stuff :refresh true))))))




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
    (create-station-anonymous-index)
    (catch Exception e (println "Creating Index failed, possibly it already exists?")))

  (try 
    (create-station-mapping)
    (catch Exception e (println "Creating mapping failed?")))

  (try 
    (create-station-anonymous-mapping)
    (catch Exception e (println "Creating mapping failed?")))

  (try 
    (create-user-mapping)
    (catch Exception e (println "Creating mapping failed?")))

  (println "inside th ebulkmsome" target my-worker-name total-loops batch-size)

  (try
    (dorun total-loops (repeatedly #(digest-next-scheduled-users-data batch-size)))
    (catch Exception e (println "Caught exception in the user indexing... I wont print it now, maybe look into it on the server?")))


  (dorun total-loops (repeatedly #(digest-next-scheduled-stations-data batch-size)))

  (println "done"))

  








