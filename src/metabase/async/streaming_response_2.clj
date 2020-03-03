(ns metabase.async.streaming-response-2
  (:require clj-http.client
            [metabase.util :as u]
            ring.adapter.jetty
            [ring.util.servlet :as servlet])
  (:import [javax.servlet.http HttpServletRequest HttpServletResponse]
           org.eclipse.jetty.server.handler.AbstractHandler
           org.eclipse.jetty.server.Request))

(set! *warn-on-reflection* true)

(defn- health-endpoint [^HttpServletRequest request ^HttpServletResponse response]
  (with-open [^org.eclipse.jetty.server.HttpOutput os (.getOutputStream response)]
    (.write os (byte-array (map int "{\"status\":\"ok\"}\n"))))
  (.. request getAsyncContext complete))

#_(defn- slow-endpoint [^HttpServletRequest request ^HttpServletResponse response]
    (with-open [^org.eclipse.jetty.server.HttpOutput os (.getOutputStream response)]
      (.write os (byte-array (map int "OK\n")))
      (.flush os)
      (.softClose os)
      (println "<SLEEP>")
      (Thread/sleep 1000)
      (.reopen os)
      (.write os (byte-array (map int "OK\n")))
      (.flush os)))

(defn- write-to-response! [^javax.servlet.AsyncEvent event, ^String s]
  (.write (.. event getAsyncContext getResponse getWriter) s))

#_(defn- async-listener ^javax.servlet.AsyncListener []
  (reify javax.servlet.AsyncListener
    (onComplete [this event]
      (println "onComplete" event)      ; NOCOMMIT
      (write-to-response! event "onComplete")
      )
    (onError [this event]
      (println "onError" event)         ; NOCOMMIT
      (write-to-response! "onError")
      (write-to-response! event "onError")
      )
    (onStartAsync [this event]
      (println "onStartAsync" event)    ; NOCOMMIT
      (write-to-response! event "onStartAsync")
      )
    (onTimeout [this event]
      (println "onTimeout" event)       ; NOCOMMIT
      (write-to-response! event "onTimeout")
      )))

(defn- start! [^HttpServletRequest request ^Runnable thunk]
  (.. request getAsyncContext (start thunk)))

(defn- slow-endpoint [^HttpServletRequest request ^HttpServletResponse response]
  #_(.addListener (.getAsyncContext request) (async-listener))
  (future
    (let [^org.eclipse.jetty.server.HttpOutput os (.getOutputStream response)]
      (.write os (byte-array (map int "OK\n")))
      (.softClose os))
    (future
      (Thread/sleep 1000)
      (start! request (fn []
                        (let [^org.eclipse.jetty.server.HttpOutput os (.getOutputStream response)]
                          (.reopen os)
                          (.write os (byte-array (map int "OK\n")))
                          (.close os))
                        (.complete (.getAsyncContext request)))))))

(def ^AbstractHandler async-handler
  (proxy [AbstractHandler] []
    (handle [_ ^org.eclipse.jetty.server.Request base-request ^HttpServletRequest request ^HttpServletResponse response]
      (let [^org.eclipse.jetty.server.AsyncContextState context (doto (.startAsync request)
                                                                  (.setTimeout 15000))
            request-map           (servlet/build-request-map request)]
        (try
          ((if (= (:uri request-map) "/api/health")
             health-endpoint
             slow-endpoint) request response)
          (catch Throwable e
            (.sendError response 500 (.getMessage e))
            (.complete context))
          (finally
            (.setHandled base-request true)))))))

(defonce ^org.eclipse.jetty.server.Server server* (atom nil))

(defn- set-server! [new-server]
  (let [[old new] (reset-vals! server* new-server)]
    (when-not (= old new)
      (when old
        (println "Stop" old)
        (.stop ^org.eclipse.jetty.server.Server old))
      (when new
        (println "Start" new)
        (.start ^org.eclipse.jetty.server.Server new)))))

(set-server! (doto ^org.eclipse.jetty.server.Server (#'ring.adapter.jetty/create-server {:port 5000, :async? true, :min-threads 4, :max-threads 5})
               (.setHandler async-handler)))

(defn- get-health []
  (u/with-timeout 15000
    (:body (clj-http.client/get "http://localhost:5000/api/health"))))

(defn- get-slow []
  (u/with-timeout 15000
    (:body (clj-http.client/get "http://localhost:5000/"))))

(defn- x []
  (dotimes [_ 8]
    (future
      (let [response (get-slow)]
        (locking println (println "[SLOW request finished]:" (pr-str response))))))
  (Thread/sleep 200)
  (get-health))
