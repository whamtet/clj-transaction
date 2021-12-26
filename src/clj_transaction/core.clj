(ns clj-transaction.core
  (:refer-clojure :exclude [update remove])
  (:require
    [monger.collection :as mc]))

(def needed-functions '(find-one-as-map insert update remove find-one-as-map find-maps))
(defmacro defpartials-for [db]
  `(do
     ~@(for [f needed-functions]
         `(defn ~f [& args#]
            (apply ~(symbol "mc" (str f)) ~db args#)))))

(declare db)
(defpartials-for db)

(defn- find-binding [coll binding match]
  (cond
    (= '_ binding) nil
    (vector? binding) [(first binding) `(~'find-maps ~coll ~match)]
    :else [binding `(~'find-one-as-map ~coll ~match)]))

(defn- update-binding [coll match]
  (if match
    `(fn [doc#] (update ~coll ~match doc#))
    `(fn [match# doc#] (update ~coll match# doc#))))
(defn- update-multi-binding [coll match]
  (if match
    `(fn [doc#] (update ~coll ~match doc# {:multi true}))
    `(fn [match# doc#] (update ~coll match# doc# {:multi true}))))
(defn- upsert-binding [coll match]
  (if match
    `(fn [doc#] (update ~coll ~match doc# {:upsert true}))
    `(fn [match# doc#] (update ~coll match# doc# {:upsert true}))))
(defn- upsert-multi-binding [coll match]
  (if match
    `(fn [doc#] (update ~coll ~match doc# {:upsert true :multi true}))
    `(fn [match# doc#] (update ~coll match# doc# {:upsert true :multi true}))))

(defmacro transact [coll [binding match] & body]
  `(let [~@(find-binding coll binding match)
         ~'insert (partial ~'insert ~coll)
         ~'remove (partial ~'remove ~coll)
         ~'update ~(update-binding coll match)
         ~'update-multi ~(update-multi-binding coll match)
         ~'upsert ~(upsert-binding coll match)
         ~'upsert-multi ~(upsert-multi-binding coll match)]
        ~@body))