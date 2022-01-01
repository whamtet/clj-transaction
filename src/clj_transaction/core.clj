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

(defn update-match [coll match m]
  (if match
    `(fn [doc#] (update ~coll ~match doc# ~m))
    `(fn [match# doc#] (update ~coll match# doc# ~m))))

(defmacro transact [coll [binding match] & body]
  (let [match (if (symbol? match) {:_id match} match)]
    `(let [~@(find-binding coll binding match)
           ~'insert (partial ~'insert ~coll)
           ~'remove (partial ~'remove ~coll)
           ~'update ~(update-match coll match {})
           ~'update-multi ~(update-match coll match {:multi true})
           ~'upsert ~(update-match coll match {:upsert true})
           ~'upsert-multi ~(update-match coll match {:upsert true :multi true})]
          ~@body)))