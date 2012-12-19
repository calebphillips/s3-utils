(ns s3-utils.core
  (:require [clojure.java.io :refer (file)])
  (:import [com.amazonaws
            AmazonClientException
            AmazonServiceException
            auth.AWSCredentials
            auth.BasicAWSCredentials]
           [com.amazonaws.services.s3
            AmazonS3
            AmazonS3Client
            model.ListObjectsRequest
            model.ObjectListing
            model.S3ObjectSummary
            model.ProgressEvent
            model.ProgressListener
            model.PutObjectRequest
            transfer.TransferManager
            transfer.Upload]))

(defn credentials []
  (BasicAWSCredentials.
   (System/getenv "AWS_ACCESS_KEY_ID")
   (System/getenv "AWS_SECRET_ACCESS_KEY")))

(defn create-list-request [bucket-name]
  (doto (ListObjectsRequest.)
    (.withBucketName bucket-name)))

(defn list-objects [bucket-name]
  (println "Listing objects")
  (let [s3client (AmazonS3Client. (credentials))
        req (create-list-request bucket-name)]
    (loop [listing (.listObjects s3client req)]
      (doseq [os (.getObjectSummaries listing)]
        (println (str "Object: " (.getKey os))))
      (if (.isTruncated listing)
        (recur (.listObjects s3client req))))))

(defn -h [n]
  (let [sizes {0 "B" 1 "K" 2 "M" 3 "G"}]
    (loop [n n its 0]
      (if (< n 1024)
        (str n (sizes its))
        (recur (float (/ n 1024)) (inc its))))))

(defn create-listener []
  (let [bytes (atom 0)]
    (proxy [ProgressListener] []
      (progressChanged [event]
        (swap! bytes + (.getBytesTransfered event))
        (println (str "Bytes transferred so far: " (-h @bytes)))))))

(defn upload [bucket-name key file]
  (println "Uploading")
  (let [tm (TransferManager. (credentials))
        req (doto (PutObjectRequest. bucket-name key file)
              (.setProgressListener (create-listener)))
        upload (.upload tm req)]
    (.waitForCompletion upload)))

(defn -main [bucket file-path]
  (let [f (file file-path)
        key (.getName f)]
    (upload bucket key f)))
