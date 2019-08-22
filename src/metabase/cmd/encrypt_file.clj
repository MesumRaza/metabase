(ns metabase.cmd.encrypt-file
  (:require [metabase.cmd.encrypt-asymm :as asymm]
            [clojure.java.io :as io]
            [metabase.cmd.encrypt-symm :as symm])
  (:import (java.util.zip ZipEntry ZipOutputStream GZIPInputStream ZipInputStream)))

;;TODO maybe rename this cmd to secure-dump or something since it encrypts, decrypts, zips, unzips, uploads, downloads, etc

(defn- same-contents? [file1 file2]
  (= (slurp (io/file file1))
     (slurp (io/file file2))))

(defn decrypt-file
  [{:keys [enc-dump-path outpath enc-secret-path key-spec]}]
  (assert (and enc-dump-path outpath) "Needs both source and target file to decrypt.")
  (try
    (let [enc-secret-key (slurp (io/file enc-secret-path))
          ;;TODO pub-key here just for verification purposes
          pub-key (or (:pub-key-path key-spec))
          private-key (or (:private-key-path key-spec))
          _ (println "sec enc: " enc-secret-key)
          secret-key (asymm/decrypt enc-secret-key (asymm/private-key private-key))
          enc-payload (slurp (io/file enc-dump-path))
          enc-payload-decrypted (symm/decrypt enc-payload secret-key)]
      (println "Writing decrypted contents to: " outpath)
      (spit (io/file outpath) enc-payload-decrypted))
    (catch Exception e
      (println "Error: " e))))

(defn encrypt-file
  [{:keys [inpath enc-dump-path enc-secret-path key-spec]}]
  (assert (and inpath enc-dump-path) "Needs both source and target file to encrypt.")
  (try
    (let [secret-key (or (:secret-key key-spec))
          pub-key (or (:pub-key-path key-spec))
          ;;TODO private key here is only for validation, actual fn would not need it
          private-key (or (:private-key-path key-spec))
          payload (slurp (io/file inpath))
          enc-payload (symm/encrypt payload secret-key)
          enc-payload-decrypted (symm/decrypt enc-payload secret-key)
          enc-secret (asymm/encrypt secret-key (asymm/pub-key pub-key))
          dec-secret (asymm/decrypt enc-secret (asymm/private-key private-key))
          enc-out-path enc-dump-path
          enc-secret-dec-path (str enc-dump-path ".secret.dec.debug")
          enc-out-dec-path (str enc-dump-path ".dec.debug")]
      (println "Writing encrypted content")
      (spit (io/file enc-out-path) enc-payload)
      (println "Writing encrypted secret")
      (spit (io/file enc-secret-path) enc-secret)
      (println "Writing decrypted secret")
      (spit (io/file enc-secret-dec-path) dec-secret)
      (println "Writing decrypted content")
      (spit (io/file enc-out-dec-path) enc-payload-decrypted)
      (assert (same-contents? inpath enc-out-dec-path) "Encrypted contents decrypted again have the same contents.")
      (assert (= secret-key dec-secret) "Encrypted secret descrypted again should have same contents."))
    (catch Exception e
      (println "Error: " e))))


(defmacro ^:private with-entry
  [zip entry-name & body]
  `(let [^ZipOutputStream zip# ~zip]
     (.putNextEntry zip# (ZipEntry. ~entry-name))
     ~@body
     (flush)
     (.closeEntry zip#)))


(defn zip-secure-dump
  [{:keys [enc-dump-path enc-secret-path zip-path]}]
  (println "Zipping " enc-dump-path enc-secret-path " --> " zip-path)
  (with-open [output (ZipOutputStream. (io/output-stream zip-path))
              input-dump (io/input-stream enc-dump-path)
              input-secret (io/input-stream enc-secret-path)]
    (with-entry output "dump.enc"
                (io/copy input-dump output))
    (with-entry output "secret.enc"
                (io/copy input-secret output))))

(defn unzip-secure-dump [{:keys [zip-path dump-path secret-path]}]
  (println "Unzipping " zip-path " --> " dump-path secret-path)
  (let [stream (->
                 (io/input-stream zip-path)
                 (ZipInputStream.))]
    (loop [entry (.getNextEntry stream)]
      (when entry
        (let [entry-name (.getName entry)]
          (println "Unzipping " entry-name)
          (case entry-name
            "dump.enc" (clojure.java.io/copy stream (clojure.java.io/file dump-path))
            "secret.enc" (clojure.java.io/copy stream (clojure.java.io/file secret-path)))
          (recur (.getNextEntry stream)))))))


(defn DEMO []
  (let [enc-dump-path "./keys/dump__file.txt.aes.enc"
        enc-secret-path "./keys/dump_secret__file.txt.aes.enc"
        zip-path "./keys/dump.zip"]

    (encrypt-file {:inpath          "./keys/file.txt"
                   :enc-dump-path   enc-dump-path
                   :enc-secret-path enc-secret-path
                   :key-spec        {:secret-key       "mysecretkey"
                                     :pub-key-path     "./keys/mig_pub_key"
                                     :private-key-path "./keys/mig_private_key"}})

    (zip-secure-dump {:enc-dump-path   enc-dump-path
                      :enc-secret-path enc-secret-path
                      :zip-path        zip-path})
    (unzip-secure-dump {:zip-path    zip-path
                        :dump-path   "./keys/dump__unzip.enc"
                        :secret-path "./keys/dump_secret__unzip.enc"})

    (decrypt-file {:enc-dump-path   "./keys/dump__unzip.enc"
                   :outpath         "./keys/result_file.txt.aes.enc.dec"
                   :enc-secret-path enc-secret-path
                   :key-spec        {:pub-key-path     "./keys/mig_pub_key"
                                     :private-key-path "./keys/mig_private_key"}})))

(DEMO)