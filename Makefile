
clean:
	mvn clean
build: clean
	mvn package
run:
	cd $(HADOOP_PATH) ; \
	$(HADOOP_PATH)/bin/hadoop jar $(JAR_FILE) org.example.WordCountRemote
