(ns metabase.server
  (:require [clojure
             [core :as core]
             [string :as str]]
            [clojure.tools.logging :as log]
            [medley.core :as m]
            [metabase
             [config :as config]
             [util :as u]]
            [metabase.util.i18n :refer [trs]]
            [potemkin.types :as p.types]
            [ring.adapter.jetty :as ring.jetty]
            [ring.util.servlet :as ring.servlet])
  (:import javax.servlet.AsyncContext
           [javax.servlet.http HttpServletRequest HttpServletResponse]
           [org.eclipse.jetty.server Request Server]
           org.eclipse.jetty.server.handler.AbstractHandler))

(defn- jetty-ssl-config []
  (m/filter-vals
   some?
   {:ssl-port       (config/config-int :mb-jetty-ssl-port)
    :keystore       (config/config-str :mb-jetty-ssl-keystore)
    :key-password   (config/config-str :mb-jetty-ssl-keystore-password)
    :truststore     (config/config-str :mb-jetty-ssl-truststore)
    :trust-password (config/config-str :mb-jetty-ssl-truststore-password)}))

(defn- jetty-config []
  (cond-> (m/filter-vals
           some?
           {:port          (config/config-int :mb-jetty-port)
            :host          (config/config-str :mb-jetty-host)
            :max-threads   (config/config-int :mb-jetty-maxthreads)
            :min-threads   (config/config-int :mb-jetty-minthreads)
            :max-queued    (config/config-int :mb-jetty-maxqueued)
            :max-idle-time (config/config-int :mb-jetty-maxidletime)})
    (config/config-str :mb-jetty-daemon) (assoc :daemon? (config/config-bool :mb-jetty-daemon))
    (config/config-str :mb-jetty-ssl)    (-> (assoc :ssl? true)
                                             (merge (jetty-ssl-config)))))

(defn- log-config [jetty-config]
  (log/info (trs "Launching Embedded Jetty Webserver with config:")
            "\n"
            (u/pprint-to-str (m/filter-keys
                              #(not (str/includes? % "password"))
                              jetty-config))))

(defonce ^:private instance*
  (atom nil))

(defn instance
  "*THE* instance of our Jetty web server, if there currently is one."
  ^Server []
  @instance*)

(p.types/defprotocol+ Response
  (respond* [this ^HttpServletRequest request ^HttpServletResponse response request-map response-map]))

(extend-protocol Response
  Object
  (respond* [_ ^HttpServletRequest request response _ response-map]
    (ring.servlet/update-servlet-response response (.getAsyncContext request) response-map))

  nil
  (respond* [_ ^HttpServletRequest request response _ response-map]
    (ring.servlet/update-servlet-response response (.getAsyncContext request) response-map)))

(defn- ^AbstractHandler async-proxy-handler [handler timeout]
  (proxy [AbstractHandler] []
    (handle [_ ^Request base-request ^HttpServletRequest request ^HttpServletResponse response]
      (let [^AsyncContext context (doto (.startAsync request)
                                    (.setTimeout timeout))
            request-map           (ring.servlet/build-request-map request)
            raise                 (fn [^Throwable e]
                                    (println "e:" e) ; NOCOMMIT
                                    (.setHandled base-request true)
                                    (.sendError response 500 (.getMessage e))
                                    (.complete context))
            respond               (fn [response-map]
                                    (try
                                      (respond* (:body response-map) request response request-map response-map)
                                      (catch Throwable e
                                        (raise e))))]
        (try
          (handler request-map respond raise)
          (.setHandled base-request true)
          (catch Throwable e
            (raise e)))))))

(defn create-server
  "Create a new async Jetty server with `handler` and `options`. Handy for creating the real Metabase web server, and
  creating one-off web servers for tests and REPL usage."
  ^Server [handler options]
  (doto ^Server (#'ring.jetty/create-server (assoc options :async? true))
    (.setHandler
     (async-proxy-handler
      handler
      ;; TODO - I suppose the default value should be moved to the `metabase.config` namespace?
      (or (config/config-int :mb-jetty-async-response-timeout)
          (u/minutes->ms 10))))))

(defn start-web-server!
  "Start the embedded Jetty web server. Returns `:started` if a new server was started; `nil` if there was already a
  running server.

    (start-web-server! #'metabase.handler/app)"
  [handler]
  (when-not (instance)
    ;; NOTE: we always start jetty w/ join=false so we can start the server first then do init in the background
    (let [config     (jetty-config)
          new-server (create-server handler config)]
      (log-config config)
      ;; Only start the server if the newly created server becomes the official new server
      ;; Don't JOIN yet -- we're doing other init in the background; we can join later
      (when (compare-and-set! instance* nil new-server)
        (.start new-server)
        :started))))

(defn stop-web-server!
  "Stop the embedded Jetty web server. Returns `:stopped` if a server was stopped, `nil` if there was nothing to stop."
  []
  (let [[^Server old-server] (reset-vals! instance* nil)]
    (when old-server
      (log/info (trs "Shutting Down Embedded Jetty Webserver"))
      (.stop old-server)
      :stopped)))
