
clean:
	mvn clean
build: clean
	mvn package
deploy:
	cd $(HADOOP_HOME) ; \
	$(HADOOP_HOME)/bin/hadoop jar $(JAR_PATH) org.example.WordCountRemote
