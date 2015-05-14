(defproject storm-lib "1.0"
  :java-source-paths ["src/jvm"]
  :aot :all
  :dependencies [[org.apache.hadoop/hadoop-core "1.0.3"]
		 [org.apache.accumulo/accumulo-core "1.4.2"]]

  :profiles {:dev {:dependencies 
                   [[org.apache.hadoop/hadoop-core "1.0.3"]
                    [org.apache.accumulo/accumulo-core "1.4.2"]
                    [org.clojure/clojure "1.6.0"]
                    [storm "0.8.0"]]}})
