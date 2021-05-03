# apache-hadoop

Repository practicing map reduce using hadoop

## How to's

### build

```
make build
```

### deploy

Environment variables used by application at runtime.

| Name | Description | Default |
| ---- | ----- | ----- |
| HADOOP_HOME       | Directory to hadoop                    | /opt/hadoop-3.2.2 |
| REMOTE_RM_ADDRESS | Set to *yarn.resourcemanager.address*  | 192.168.11.20:
| REMOTE_NN_ADDRESS | Set to  *fs.defaultFS*                 |
| INPUT_PATH        |
| OUTPUT_PATH       |
| JAR_PATH          |
| CLASS_NAME        |

```
source appenv.sh
make deploy
```
